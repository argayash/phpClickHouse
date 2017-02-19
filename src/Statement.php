<?php

namespace ClickHouseDB;

use Curler\Request;
use Curler\Response;

/**
 * Class Statement
 * @package ClickHouseDB
 */
class Statement
{
    /**
     * @var
     */
    protected $rawData;

    /**
     * @var int
     */
    protected $httpCode = -1;

    /**
     * @var Request|null
     */
    protected $request;

    /**
     * @var bool
     */
    protected $init = false;

    /**
     * @var Query
     */
    protected $query;

    /**
     * @var string
     */
    protected $sql;

    /**
     * @var array
     */
    protected $meta = [];

    /**
     * @var array
     */
    protected $data = [];

    /**
     * @var array
     */
    protected $totals = [];

    /**
     * @var array
     */
    protected $extremes = [];

    /**
     * @var int
     */
    protected $rows = 0;

    /**
     * @var int
     */
    protected $rows_before_limit_at_least = 0;

    /**
     * @var
     */
    protected $rawResult;

    /**
     * @var array
     */
    protected $arrayData = [];

    /**
     * @var array
     */
    protected $statistics;

    /**
     * Statement constructor.
     *
     * @param Request $request
     */
    public function __construct(Request $request)
    {
        $this->setRequest($request);
        $this->query = $this->request->getRequestExtendedInfo('query');
        $this->sql = $this->request->getRequestExtendedInfo('sql');
    }

    /**
     * @return Request
     */
    public function getRequest()
    {
        return $this->request;
    }

    /**
     * @param Request $request
     *
     * @return self
     */
    public function setRequest(Request $request)
    {
        $this->request = $request;

        return $this;
    }

    /**
     * @return Response
     */
    protected function response()
    {
        return $this->getRequest()->response();
    }

    public function responseInfo()
    {
        return $this->response()->info();
    }

    /**
     * @return mixed|string
     */
    public function sql()
    {
        return $this->sql;
    }

    /**
     * @param $body
     *
     * @return array|bool
     */
    protected function parseErrorClickHouse($body)
    {
        $body = trim($body);
        $mathes = [];

        // Code: 115, e.displayText() = DB::Exception: Unknown setting readonly[0], e.what() = DB::Exception
        // Code: 192, e.displayText() = DB::Exception: Unknown user x, e.what() = DB::Exception
        // Code: 60, e.displayText() = DB::Exception: Table default.ZZZZZ doesn't exist., e.what() = DB::Exception

        if (preg_match("%Code: (\d+),\se\.displayText\(\) \=\s*DB\:\:Exception\s*:\s*(.*)\,\s*e\.what.*%ius", $body,
            $mathes)) {
            return ['code' => $mathes[1], 'message' => $mathes[2]];
        }

        return false;
    }

    /**
     * @return bool
     */
    public function error()
    {
        if (!$this->isError()) {
            return false;
        }

        $body = $this->response()->body();
        $error_no = $this->response()->error_no();
        $error = $this->response()->error();

        if (!$error_no && !$error) {
            $parse = $this->parseErrorClickHouse($body);

            if ($parse) {
                throw new DatabaseException($parse['message'] . "\nIN:" . $this->sql(), $parse['code']);
            } else {
                $code = $this->response()->http_code();
                $message = "HttpCode:" . $this->response()->http_code() . " ; " . $this->response()->error() . " ;" . $body;
            }
        } else {
            $code = $error_no;
            $message = $this->response()->error();
        }

        throw new QueryException($message, $code);
    }

    /**
     * @return bool
     */
    public function isError()
    {
        return ($this->response()->http_code() !== 200 || $this->response()->error_no());
    }

    /**
     *
     * @return bool
     */
    protected function check()
    {
        if (!$this->request->isResponseExists()) {
            throw new QueryException('Not have response');
        }

        if ($this->isError()) {
            $this->error();
        }

        return true;
    }

    /**
     * @return bool
     */
    protected function init()
    {
        if ($this->init) {
            return false;
        }

        $this->check();

        $this->rawData = $this->request->getFormatHandler()->getResponseDataArray();

        if (!$this->rawData) {
            $this->init = true;
            return false;
        }

        foreach (['meta', 'data', 'totals', 'extremes', 'rows', 'rows_before_limit_at_least', 'statistics'] as $key) {
            if (isset($this->rawData[$key])) {
                $this->{$key} = $this->rawData[$key];
            }
        }

        if (empty($this->meta)) {
            throw  new QueryException('Can`t find meta');
        }

        $this->arrayData = [];
        foreach ($this->data as $rows) {
            $r = [];

            foreach ($this->meta as $meta) {
                $r[$meta['name']] = $rows[$meta['name']];
            }

            $this->arrayData[] = $r;
        }

        return true;
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function extremes()
    {
        $this->init();
        return $this->extremes;
    }

    /**
     * @return mixed
     */
    public function totalTimeRequest()
    {
        $this->check();
        return $this->response()->total_time();

    }

    /**
     * @return array
     * @throws \Exception
     */
    public function extremesMin()
    {
        $this->init();

        if (empty($this->extremes['min'])) {
            return [];
        }

        return $this->extremes['min'];
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function extremesMax()
    {
        $this->init();

        if (empty($this->extremes['max'])) {
            return [];
        }

        return $this->extremes['max'];
    }

    /**
     * @return mixed
     */
    public function totals()
    {
        $this->init();
        return $this->totals;
    }

    /**
     *
     */
    public function dumpRaw()
    {
        print_r($this->rawData);
    }

    /**
     *
     */
    public function dump()
    {
        $this->request->dump();
        $this->response()->dump();
    }

    /**
     * @return bool
     */
    public function countAll()
    {
        $this->init();

        return $this->rows_before_limit_at_least;
    }

    /**
     * @param bool $key
     *
     * @return array|mixed|null
     */
    public function statistics($key = false)
    {
        $this->init();
        if ($key) {
            if (!is_array($this->statistics)) {
                return null;
            }
            if (!isset($this->statistics[$key])) {
                return null;
            }
            return $this->statistics[$key];
        }
        return $this->statistics;
    }

    /**
     * @return int
     */
    public function count()
    {
        $this->init();
        return $this->rows;
    }

    /**
     * get rawJson Answer
     *
     * @return mixed
     */
    public function rawData()
    {
        if ($this->init) {
            return $this->rawData;
        }

        $this->check();

        return $this->request->getFormatHandler()->getResponseDataArray();
    }

    /**
     * @param bool $key
     *
     * @return mixed|null
     */
    public function fetchOne($key = false)
    {
        $this->init();

        if (isset($this->arrayData[0])) {
            if ($key) {
                if (isset($this->arrayData[0][$key])) {
                    return $this->arrayData[0][$key];
                } else {
                    return null;
                }
            }

            return $this->arrayData[0];
        }

        return null;
    }

    /**
     * @param $path
     *
     * @return array
     */
    public function rowsAsTree($path)
    {
        $this->init();

        $out = [];
        foreach ($this->arrayData as $row) {
            $d = $this->arrayToTree($row, $path);
            $out = array_replace_recursive($d, $out);
        }

        return $out;
    }

    /**
     * Return size_upload,upload_content,speed_upload,time_request
     *
     * @return array
     */
    public function info_upload()
    {
        $this->check();
        return [
            'size_upload' => $this->response()->size_upload(),
            'upload_content' => $this->response()->upload_content_length(),
            'speed_upload' => $this->response()->speed_upload(),
            'time_request' => $this->response()->total_time()
        ];
    }

    /**
     * Return size_upload,upload_content,speed_upload,time_request,starttransfer_time,size_download,speed_download
     *
     * @return array
     */
    public function info()
    {
        $this->check();
        return [
            'starttransfer_time' => $this->response()->starttransfer_time(),
            'size_download' => $this->response()->size_download(),
            'speed_download' => $this->response()->speed_download(),
            'size_upload' => $this->response()->size_upload(),
            'upload_content' => $this->response()->upload_content_length(),
            'speed_upload' => $this->response()->speed_upload(),
            'time_request' => $this->response()->total_time()
        ];
    }

    /**
     * @return array
     */
    public function rows()
    {
        $this->init();
        return $this->arrayData;
    }

    /**
     * @param $arr
     * @param mixed $path
     *
     * @return array
     */
    protected function arrayToTree($arr, $path = null)
    {
        if (is_array($path)) {
            $keys = $path;
        } else {
            $args = func_get_args();
            array_shift($args);

            if (count($args) < 2) {
                $separator = '.';
                $keys = explode($separator, $path);
            } else {
                $keys = $args;
            }
        }

        $tree = $arr;
        while (count($keys)) {
            $key = array_pop($keys);
            $val = $key;

            if (isset($arr[$key])) {
                $val = $arr[$key];
            }

            $tree = [$val => $tree];
        }

        return $tree;
    }
}