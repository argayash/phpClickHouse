<?php

namespace ClickHouseDB\Transport;

use ClickHouseDB\Query;
use ClickHouseDB\Settings;
use ClickHouseDB\Statement;
use ClickHouseDB\WhereInFile;
use ClickHouseDB\WriteToFile;
use Curler\CurlerRolling;
use Curler\Request;

/**
 * Class Http like simpleCurl
 * @package ClickHouseDB\Transport
 */
class Http
{
    /**
     * @var string
     */
    protected $username;

    /**
     * @var string
     */
    protected $password;

    /**
     * @var string
     */
    protected $host = '';

    /**
     * @var int
     */
    protected $port = 0;

    /**
     * @var bool
     */
    protected $verbose = false;

    /**
     * @var CurlerRolling
     */
    protected $curler = false;

    /**
     * @var Settings
     */
    protected $settings = false;

    /**
     * @var array
     */
    protected $queryDegenerations = [];

    /**
     * Количество секунд ожидания при попытке соединения
     *
     * @var int
     */
    protected $connectTimeOut = 5;

    /**
     * Http constructor.
     *
     * @param string $host
     * @param int $port
     * @param string $username
     * @param string $password
     */
    public function __construct(string $host, int $port, string $username, string $password = '')
    {
        $this->setHost($host, $port);

        $this->username = $username;
        $this->password = $password;

        $this->setSettings(new Settings($this));
        $this->setCurler(new CurlerRolling());
    }


    public function setCurler(CurlerRolling $curlerRolling)
    {
        $this->curler = $curlerRolling;
    }

    /**
     * @return CurlerRolling
     */
    public function getCurler()
    {
        return $this->curler;
    }

    /**
     * @param $host
     * @param int $port
     */
    public function setHost($host, $port = -1)
    {
        if ($port > 0) {
            $this->port = $port;
        }

        $this->host = $host;
    }

    /**
     * @return string
     */
    public function getUri()
    {
        return 'http://' . $this->host . ':' . $this->port;
    }

    /**
     * @return Settings
     */
    public function settings()
    {
        if (!$this->getSettings()) {
            throw  new \InvalidArgumentException('Empty settings class');
        }
        return $this->settings;
    }

    /**
     * @return Settings
     */
    public function getSettings()
    {
        return $this->settings;
    }

    /**
     * @param Settings $settings
     */
    public function setSettings(Settings $settings)
    {
        $this->settings = $settings;
    }

    /**
     * @param $flag
     *
     * @return mixed
     */
    public function verbose($flag)
    {
        $this->verbose = $flag;
        return $flag;
    }

    /**
     * @param array $params
     *
     * @return string
     */
    protected function getUrl(array $params = []): string
    {
        $settings = $this->settings()->getSettings();

        if (is_array($params) && count($params)) {
            $settings = array_merge($settings, $params);
        }

        if ($this->settings()->isReadOnlyUser()) {
            unset($settings['extremes'], $settings['readonly'], $settings['enable_http_compression'], $settings['max_execution_time']);
        }

        return $this->getUri() . '?' . http_build_query($settings);
    }

    /**
     * @param $extendInfo
     *
     * @return Request
     */
    protected function newRequest($extendInfo)
    {
        $request = new Request();
        $request->auth($this->username, $this->password)->POST()->setRequestExtendedInfo($extendInfo);

        if ($this->settings()->isEnableHttpCompression()) {
            $request->httpCompression(true);
        }

        $request->timeOut($this->settings()->getTimeOut());
        $request->connectTimeOut($this->getConnectTimeOut())->keepAlive();// one sec
        $request->verbose($this->verbose);

        return $request;
    }

    /**
     * @param Query $query
     * @param array $urlParams
     * @param bool $queryAsString
     *
     * @return Request
     */
    protected function makeRequest(Query $query, array $urlParams = [], $queryAsString = false)
    {
        $sql = $query->toSql();

        if ($queryAsString) {
            $urlParams['query'] = $sql;
        }

        $url = $this->getUrl($urlParams);

        $extendInfo = [
            'sql' => $sql,
            'query' => $query
        ];

        $request = $this->newRequest($extendInfo);
        $request->url($url);

        if (!$queryAsString) {
            $request->parametersJson($sql);
        }
        if ($this->settings()->isEnableHttpCompression()) {
            $request->httpCompression(true);
        }

        return $request;
    }

    /**
     * @param $sql
     *
     * @return Request
     */
    public function writeStreamData($sql)
    {
        $query = new Query($sql);

        $url = $this->getUrl([
            'readonly' => 0,
            'query' => $query->toSql()
        ]);

        $extendInfo = [
            'sql' => $sql,
            'query' => $query
        ];

        $request = $this->newRequest($extendInfo);
        $request->url($url);
        return $request;
    }


    /**
     * @param $sql
     * @param $fileName
     *
     * @return Statement
     */
    public function writeAsyncCSV($sql, $fileName)
    {
        $query = new Query($sql);

        $url = $this->getUrl([
            'readonly' => 0,
            'query' => $query->toSql()
        ]);

        $extendInfo = [
            'sql' => $sql,
            'query' => $query
        ];

        $request = $this->newRequest($extendInfo);
        $request->url($url);

        $request->setCallbackFunction(function (Request $request) {
            fclose($request->getInfileHandle());
        });

        $request->setInfile($fileName);

        $this->getCurler()->addQueLoop($request);

        return new Statement($request);
    }

    /**
     * @return int
     */
    public function getCountPendingQueue()
    {
        return $this->curler->countPending();
    }

    /**
     * Количество секунд ожидания
     *
     * @param int $connectTimeOut
     */
    public function setConnectTimeOut($connectTimeOut)
    {
        $this->connectTimeOut = $connectTimeOut;
    }

    /**
     * Количество секунд ожидания
     *
     * @return int
     */
    public function getConnectTimeOut()
    {
        return $this->connectTimeOut;
    }

    /**
     * @param Query $query
     * @param null $whereInFile
     * @param null $writeToFile
     *
     * @return Request
     */
    public function getRequestRead(Query $query, $whereInFile = null, $writeToFile = null)
    {
        $urlParams = ['readonly' => 1];
        $queryAsString = false;
        // ---------------------------------------------------------------------------------
        if ($whereInFile instanceof WhereInFile && $whereInFile->size()) {
            // $request = $this->prepareSelectWhereIn($request, $whereInFile);
            $structure = $whereInFile->fetchUrlParams();
            // $structure = [];
            $urlParams = array_merge($urlParams, $structure);
            $queryAsString = true;
        }
        // ---------------------------------------------------------------------------------
        // if result to file
        if ($writeToFile instanceof WriteToFile && $writeToFile->fetchFormat()) {
            $query->setFormat($writeToFile->fetchFormat());
            unset($urlParams['extremes']);
        }
        // ---------------------------------------------------------------------------------
        // makeRequest read
        $request = $this->makeRequest($query, $urlParams, $queryAsString);
        // ---------------------------------------------------------------------------------
        // attach files
        if ($whereInFile instanceof WhereInFile && $whereInFile->size()) {
            $request->attachFiles($whereInFile->fetchFiles());
        }
        // ---------------------------------------------------------------------------------
        // result to file
        if ($writeToFile instanceof WriteToFile && $writeToFile->fetchFormat()) {

            $fout = fopen($writeToFile->fetchFile(), 'w');
            $isGz = $writeToFile->getGzip();

            if ($isGz) {
                // write gzip header
//                "\x1f\x8b\x08\x00\x00\x00\x00\x00"
//                fwrite($fout, "\x1F\x8B\x08\x08".pack("V", time())."\0\xFF", 10);
                fwrite($fout, "\x1f\x8b\x08\x00\x00\x00\x00\x00");
                // write the original file name
//                $oname = str_replace("\0", "", basename($writeToFile->fetchFile()));
//                fwrite($fout, $oname."\0", 1+strlen($oname));

            }


            $request->setResultFileHandle($fout, $isGz)->setCallbackFunction(function (Request $request) {
                fclose($request->getResultFileHandle());
            });
        }
        // ---------------------------------------------------------------------------------
        return $request;
    }

    public function cleanQueryDegeneration()
    {
        $this->queryDegenerations = [];
        return true;
    }

    public function addQueryDegeneration(Query\Degeneration $degeneration)
    {
        $this->queryDegenerations[] = $degeneration;
        return true;
    }

    /**
     * @param Query $query
     *
     * @return Request
     */
    public function getRequestWrite(Query $query)
    {
        $urlParams = ['readonly' => 0];
        return $this->makeRequest($query, $urlParams);
    }

    /**
     * @param $sql
     * @param $bindings
     *
     * @return Query
     */
    protected function prepareQuery($sql, $bindings)
    {
        foreach ($this->queryDegenerations as $degeneration) {
            $degeneration->bindParams($bindings);
        }

        return new Query($sql, $this->queryDegenerations);
    }

    /**
     * @param $sql
     * @param array $bindings
     * @param $whereInFile
     * @param $writeToFile
     *
     * @return Request
     */
    protected function prepareSelect($sql, array $bindings = [], $whereInFile = null, $writeToFile = null): Request
    {
        if ($sql instanceof Query) {
            return $this->getRequestWrite($sql);
        }

        $query = $this->prepareQuery($sql, $bindings);
        $query->setFormat('JSON');
        return $this->getRequestRead($query, $whereInFile, $writeToFile);

    }

    /**
     * @param $sql
     * @param $bindings
     *
     * @return Request
     */
    protected function prepareWrite($sql, array $bindings = []): Request
    {
        if ($sql instanceof Query) {
            return $this->getRequestWrite($sql);
        }

        $query = $this->prepareQuery($sql, $bindings);
        return $this->getRequestWrite($query);
    }

    /**
     *
     * @return bool
     */
    public function executeAsync()
    {
        return $this->curler->execLoopWait();
    }

    /**
     * @param $sql
     * @param array $bindings
     * @param null $whereInFile
     * @param null $writeToFile
     *
     * @return Statement
     */
    public function select($sql, array $bindings = [], $whereInFile = null, $writeToFile = null): Statement
    {
        $request = $this->prepareSelect($sql, $bindings, $whereInFile, $writeToFile);
        $code = $this->curler->execOne($request);

        return new Statement($request);
    }

    /**
     * @param $sql
     * @param array $bindings
     * @param null $whereInFile
     * @param null $writeToFile
     *
     * @return Statement
     */
    public function selectAsync($sql, array $bindings = [], $whereInFile = null, $writeToFile = null): Statement
    {
        $request = $this->prepareSelect($sql, $bindings, $whereInFile, $writeToFile);
        $this->curler->addQueLoop($request);

        return new Statement($request);
    }


    /**
     * @param $sql
     * @param array $bindings
     * @param bool $exception
     *
     * @return Statement
     */
    public function write($sql, array $bindings = [], bool $exception = true): Statement
    {
        $request = $this->prepareWrite($sql, $bindings);
        $code = $this->curler->execOne($request);

        $response = new Statement($request);
        if ($exception) {
            if ($response->isError()) {
                $response->error();
            }
        }

        return $response;
    }
}
