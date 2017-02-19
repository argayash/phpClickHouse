<?php

namespace ClickHouseDB;

use ClickHouseDB\Query\Degeneration\Bindings;
use ClickHouseDB\Query\Degeneration\Conditions;
use ClickHouseDB\Transport\Http;

/**
 * Class Client
 * @package ClickHouseDB
 */
class Client
{
    const FORMAT_TAB_SEPARATED = 'TabSeparated';
    const FORMAT_TAB_SEPARATED_WITH_NAMES = 'TabSeparatedWithNames';
    const FORMAT_CSV = 'CSV';
    const FORMAT_CSV_WITH_NAMES = 'CSVWithNames';

    const PARTITIONS_CHUNK_SIZE = 100;

    /**
     * @var Http
     */
    protected $transport;

    /**
     * @var string
     */
    protected $connectUsername;

    /**
     * @var string
     */
    protected $connectPassword;

    /**
     * @var string
     */
    protected $connectHost;

    /**
     * @var int
     */
    protected $connectPort;

    /**
     * @var bool
     */
    protected $connectUserReadonly;
    /**
     * @var array
     */
    protected static $supportedFormats = [
        self::FORMAT_TAB_SEPARATED,
        self::FORMAT_TAB_SEPARATED_WITH_NAMES,
        self::FORMAT_CSV,
        self::FORMAT_CSV_WITH_NAMES
    ];

    /**
     * Client constructor.
     *
     * @param array $connectParams
     * @param array $settings
     */
    public function __construct(array $connectParams, array $settings = [])
    {
        if (!isset($connectParams['username'])) {
            throw  new \InvalidArgumentException('not set username');
        }

        if (!isset($connectParams['password'])) {
            throw  new \InvalidArgumentException('not set password');
        }

        if (!isset($connectParams['port'])) {
            throw  new \InvalidArgumentException('not set port');
        }

        if (!isset($connectParams['host'])) {
            throw  new \InvalidArgumentException('not set host');
        }

        if (isset($connectParams['settings']) && is_array($connectParams['settings'])) {
            if (empty($settings)) {
                $settings = $connectParams['settings'];
            }
        }

        $this->connectUsername = $connectParams['username'];
        $this->connectPassword = $connectParams['password'];
        $this->connectPort = $connectParams['port'];
        $this->connectHost = $connectParams['host'];

        $this->setTransport(new Http($this->connectHost, $this->connectPort, $this->connectUsername,
            $this->connectPassword));

        $this->getTransport()->addQueryDegeneration(new Bindings());

        // apply settings to transport class
        $this->settings()->database('default');
        if (count($settings)) {
            $this->settings()->apply($settings);
        }

        if (isset($connectParams['readonly'])) {
            $this->setReadOnlyUser($connectParams['readonly']);
        }

    }

    /**
     * если у пользовалетя установленно только чтение в конфиге
     *
     * @param $flag
     */
    public function setReadOnlyUser($flag)
    {
        $this->connectUserReadonly = $flag;
        $this->settings()->setReadOnlyUser($this->connectUserReadonly);
    }

    /**
     * Очистить пред обработку запроса [шаблонизация]
     *
     * @return bool
     */
    public function cleanQueryDegeneration()
    {
        return $this->transport()->cleanQueryDegeneration();
    }

    /**
     * Добавить пред обработку запроса
     *
     * @param Query\Degeneration $degeneration
     *
     * @return bool
     */
    public function addQueryDegeneration(Query\Degeneration $degeneration)
    {
        return $this->getTransport()->addQueryDegeneration($degeneration);
    }

    /**
     * Замена :var в запросе
     *
     * @return bool
     */
    public function enableQueryConditions()
    {
        return $this->getTransport()->addQueryDegeneration(new Conditions());
    }

    /**
     * Set connection host
     *
     * @param $host
     */
    public function setHost($host)
    {

        if (is_array($host)) {
            $host = array_rand(array_flip($host));
        }

        $this->connectHost = $host;
        $this->transport()->setHost($host);
    }

    /**
     * Таймаут
     *
     * @param $timeout
     *
     * @return Settings
     */
    public function setTimeout($timeout)
    {
        return $this->settings()->maxExecutionTime($timeout);
    }

    /**
     * Таймаут
     *
     * @return mixed
     */
    public function getTimeout()
    {
        return $this->settings()->getTimeOut();
    }

    /**
     * Количество секунд ожидания
     *
     * @param int $connectTimeOut
     */
    public function setConnectTimeOut($connectTimeOut)
    {
        $this->transport()->setConnectTimeOut($connectTimeOut);
    }

    /**
     * Количество секунд ожидания
     *
     * @return int
     */
    public function getConnectTimeOut()
    {
        return $this->transport()->getConnectTimeOut();
    }


    /**
     * transport
     *
     * @return Http
     */
    public function transport()
    {
        if (!$this->getTransport()) {
            throw  new \InvalidArgumentException('Empty transport class');
        }
        return $this->getTransport();
    }

    /**
     * @return string
     */
    public function getConnectHost()
    {
        return $this->connectHost;
    }

    /**
     * @return string
     */
    public function getConnectPassword()
    {
        return $this->connectPassword;
    }

    /**
     * @return string
     */
    public function getConnectPort()
    {
        return $this->connectPort;
    }

    /**
     * @return string
     */
    public function getConnectUsername()
    {
        return $this->connectUsername;
    }

    /**
     * transport
     *
     * @return Http
     */
    public function getTransport()
    {
        return $this->transport;
    }

    /**
     * @param Http $httpTransport
     *
     * @return self
     */
    public function setTransport(Http $httpTransport)
    {
        $this->transport = $httpTransport;

        return $this;
    }


    /**
     * Режим отладки CURL
     *
     * @return mixed
     */
    public function verbose()
    {
        return $this->transport()->verbose(true);
    }

    /**
     * @return Settings
     */
    public function settings()
    {
        return $this->transport()->settings();
    }

    /**
     * Запрос на запись CREATE/DROP
     *
     * @param $sql
     * @param array $bindings
     * @param bool $exception
     *
     * @return Statement
     */
    public function write($sql, array $bindings = [], bool $exception = true)
    {
        return $this->transport()->write($sql, $bindings, $exception);
    }

    /**
     * @param $db
     *
     * @return $this
     */
    public function database($db)
    {
        $this->settings()->database($db);

        return $this;
    }

    /**
     * Логгировать запросы и писать лог в системную таблицу. <database>system</database> <table>query_log</table>
     *
     * @param bool $flag
     *
     * @return $this
     */
    public function enableLogQueries($flag = true)
    {
        $this->settings()->set('log_queries', (int)$flag);

        return $this;
    }

    /**
     * Сжимать результат, если клиент по HTTP сказал, что он понимает данные, сжатые методом gzip или deflate
     *
     * @param bool $flag
     *
     * @return $this
     */
    public function enableHttpCompression(bool $flag = true)
    {
        $this->settings()->enableHttpCompression($flag);

        return $this;
    }

    /**
     * Считать минимумы и максимумы столбцов результата. Они могут выводиться в JSON-форматах.
     *
     * @param bool $flag
     *
     * @return $this
     */
    public function enableExtremes(bool $flag = true)
    {
        $this->settings()->set('extremes', (int)$flag);
        return $this;
    }

    /**
     * SELECT
     *
     * @param Query|string $sql
     * @param array $bindings
     * @param WhereInFile $whereInFile
     * @param WriteToFile $writeToFile
     *
     * @return Statement
     */
    public function select($sql, array $bindings = [], WhereInFile $whereInFile = null, WriteToFile $writeToFile = null)
    {
        return $this->transport()->select($sql, $bindings, $whereInFile, $writeToFile);
    }

    /**
     * Исполнить запросы из очереди
     *
     * @return bool
     */
    public function executeAsync()
    {
        return $this->transport()->executeAsync();
    }

    /**
     * Подготовить запрос SELECT
     *
     * @param Query|string $sql
     * @param array $bindings
     * @param WhereInFile $whereInFile
     * @param WriteToFile $writeToFile
     *
     * @return Statement
     */
    public function selectAsync(
        $sql,
        array $bindings = [],
        WhereInFile $whereInFile = null,
        WriteToFile $writeToFile = null
    ) {
        return $this->transport()->selectAsync($sql, $bindings, $whereInFile, $writeToFile);
    }

    /**
     * SHOW PROCESSLIST
     *
     * @return array
     */
    public function showProcesslist()
    {
        return $this->select('SHOW PROCESSLIST')->rows();
    }

    /**
     * show databases
     *
     * @return array
     */
    public function showDatabases()
    {
        return $this->select('show databases')->rows();
    }

    /**
     * statement = SHOW CREATE TABLE
     *
     * @param string $table
     *
     * @return mixed
     */
    public function showCreateTable(string $table)
    {
        return $this->select('SHOW CREATE TABLE ' . $table)->fetchOne('statement');
    }

    /**
     * SHOW TABLES
     *
     * @return array
     */
    public function showTables()
    {
        return $this->select('SHOW TABLES')->rowsAsTree('name');
    }

    /**
     * Получить кол-во одновременных запросов
     *
     * @return int
     */
    public function getCountPendingQueue()
    {
        return $this->transport()->getCountPendingQueue();
    }

    /**
     * Вставить массив
     *
     * @param string $table
     * @param array $values
     * @param array $columns
     *
     * @return Statement
     */
    public function insert(string $table, array $values, array $columns = [])
    {
        $sql = 'INSERT INTO ' . $table;

        if (0 !== count($columns)) {
            $sql .= ' (' . implode(',', $columns) . ') ';
        }

        $sql .= ' VALUES ';

        foreach ($values as $row) {
            $sql .= ' (' . FormatLine::Insert($row) . '), ';
        }
        $sql = trim($sql, ', ');

        return $this->transport()->write($sql);
    }

    /**
     * insert TabSeparated files
     *
     * @param $tableName
     * @param $fileNames
     * @param $columnsArray
     *
     * @return mixed
     */
    public function insertBatchTSVFiles($tableName, $fileNames, $columnsArray)
    {
        return $this->insertBatchFiles($tableName, $fileNames, $columnsArray, self::FORMAT_TAB_SEPARATED);
    }

    /**
     *
     * @param string $tableName
     * @param array|string $fileNames
     * @param array $columnsArray
     * @param string $format ['TabSeparated','TabSeparatedWithNames','CSV','CSVWithNames']
     *
     * @return array
     */
    public function insertBatchFiles($tableName, $fileNames, array $columnsArray, $format = self::FORMAT_CSV)
    {
        if (is_string($fileNames)) {
            $fileNames = [$fileNames];
        }
        if ($this->getCountPendingQueue() > 0) {
            throw new QueryException('Queue must be empty, before insertBatch, need executeAsync');
        }

        if (!in_array($format, self::$supportedFormats, true)) {
            throw new QueryException('Format not support in insertBatchFiles');
        }

        $result = [];

        foreach ((array)$fileNames as $fileName) {
            if (!is_file($fileName) || !is_readable($fileName)) {
                throw  new QueryException('Cant read file: ' . $fileName . ' ' . (is_file($fileName) ? '' : ' is not file'));
            }

            $sql = 'INSERT INTO ' . $tableName . (!empty($columnsArray) ? (' ( ' . implode(', ',
                        $columnsArray) . ' )') : '') . ' FORMAT ' . $format;

            $result[$fileName] = $this->transport()->writeAsyncCSV($sql, $fileName);
        }

        // exec
        $exec = $this->executeAsync();

        // fetch resutl
        foreach ((array)$fileNames as $fileName) {
            if ($result[$fileName]->isError()) {
                $result[$fileName]->error();
            }
        }

        return $result;
    }

    /**
     * @param string $tableName
     * @param array $columns_array
     * @param string $format
     *
     * @return \Curler\Request
     */
    public function insertBatchStream($tableName, $columns_array, $format = self::FORMAT_CSV)
    {
        if ($this->getCountPendingQueue() > 0) {
            throw new QueryException('Queue must be empty, before insertBatch, need executeAsync');
        }

        if (!in_array($format, self::$supportedFormats, true)) {
            throw new QueryException('Format not support in insertBatchFiles');
        }

        $sql = 'INSERT INTO ' . $tableName . (!empty($columnsArray) ? (' ( ' . implode(', ',
                    $columnsArray) . ' )') : '') . ' FORMAT ' . $format;

        return $this->transport()->writeStreamData($sql);
    }


    /**
     * Размер базы
     *
     * @return mixed|null
     */
    public function databaseSize()
    {
        return $this->select('
            SELECT database,formatReadableSize(sum(bytes)) as size
            FROM system.parts
            WHERE active AND database=:database
            GROUP BY database
        ', ['database' => $this->settings()->getDatabase()])->fetchOne();
    }

    /**
     * Размер таблицы
     *
     * @param $tableName
     *
     * @return mixed
     */
    public function tableSize($tableName)
    {
        $tables = $this->tablesSize();

        if (isset($tables[$tableName])) {
            return $tables[$tableName];
        }

        return null;
    }

    /**
     * @return bool
     */
    public function ping()
    {
        $result = (int)$this->select('SELECT 1 as ping')->fetchOne('ping');
        return $result === 1;
    }

    /**
     * Размеры таблиц
     *
     * @param bool $flatList
     *
     * @return array
     */
    public function tablesSize(bool $flatList = false)
    {
        $statement = $this->select('
            SELECT table,database,
            formatReadableSize(sum(bytes)) as size,
            sum(bytes) as sizebytes,
            min(min_date) as min_date,
            max(max_date) as max_date
            FROM system.parts
            WHERE active
            GROUP BY table,database
        ');

        if ($flatList) {
            return $statement->rows();
        }

        return $statement->rowsAsTree('table');
    }

    public function isExists($database, $table)
    {
        return $this->select('
            SELECT *
            FROM system.tables 
            WHERE name=' . self::quoteName($table) . ' AND database=' . self::quoteName($database))->rowsAsTree('name');
    }

    /**
     * @param $table
     * @param int $limit
     *
     * @return array
     */
    public function partitions($table, $limit = -1)
    {
        return $this->select('
            SELECT *
            FROM system.parts 
            WHERE like(table,' . self::quoteName('%' . $table . '%') . ')  
            ORDER BY max_date ' . ($limit > 0 ? (' LIMIT ' . (int)$limit) : ''))->rowsAsTree('name');
    }

    /**
     * @param $tableName
     * @param $partition_id
     *
     * @return bool
     */
    public function dropPartition($tableName, $partition_id)
    {
        $state = $this->write('ALTER TABLE {tableName} DROP PARTITION :partion_id', [
            'tableName' => $tableName,
            'partion_id' => $partition_id
        ]);

        return true;
    }

    /**
     * @param string $tableName
     *
     * @return array
     */
    public function truncateTable(string $tableName)
    {
        $parts = $this->partitions($tableName);
        $out = [];
        foreach ($parts as $part) {
            $partId = $part['partition'];
            $out[$partId] = $this->dropPartition($tableName, $partId);
        }

        return $out;
    }

    /**
     * @param string $tableName
     * @param int $daysAgo
     * @param int $countPartitionsPerOne
     *
     * @return array
     */
    public function dropOldPartitions(
        string $tableName,
        int $daysAgo,
        int $countPartitionsPerOne = self::PARTITIONS_CHUNK_SIZE
    ) {
        $daysAgo = strtotime(date('Y-m-d 00:00:00', strtotime('-' . $daysAgo . ' day')));

        $drop = [];
        $listPartitions = $this->partitions($tableName, $countPartitionsPerOne);

        foreach ($listPartitions as $partion_id => $partition) {
            if (stripos($partition['engine'], 'mergetree') === false) {
                continue;
            }

            $min_date = strtotime($partition['min_date']);
            $max_date = strtotime($partition['max_date']);

            if ($max_date < $daysAgo) {
                $drop[] = $partition['partition'];
            }
        }

        foreach ($drop as $partition_id) {
            $this->dropPartition($tableName, $partition_id);
        }

        return $drop;
    }

    /**
     * @param string $param
     *
     * @return string
     */
    public static function quoteName(string $param)
    {
        return '\'' . $param . '\'';
    }

}