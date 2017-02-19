<?php

namespace ClickHouseDB;

use ClickHouseDB\Transport\Http;

/**
 * Class Settings
 * @package ClickHouseDB
 */
class Settings
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var array
     */
    protected $settings = [];

    /**
     * @var bool
     */
    protected $readOnlyUser = false;


    /**
     * Settings constructor.
     *
     * @param Http $client
     */
    public function __construct(Http $client)
    {
        $this->settings = [
            'extremes' => false,
            'readonly' => true,
            'max_execution_time' => 20,
            'enable_http_compression' => 0,
        ];

        $this->client = $client;
    }

    /**
     * @param string $key
     *
     * @return mixed
     */
    private function get(string $key)
    {
        return $this->settings[$key];
    }

    /**
     * @param string $key
     * @param $value
     *
     * @return $this
     */
    public function set(string $key, $value)
    {
        $this->settings[$key] = $value;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getDatabase()
    {
        return $this->get('database');
    }

    /**
     * @param $db
     *
     * @return $this
     */
    public function database($db)
    {
        $this->set('database', $db);
        return $this;
    }

    /**
     * @return mixed
     */
    public function getTimeOut()
    {
        return $this->get('max_execution_time');
    }

    /**
     * @return mixed|null
     */
    public function isEnableHttpCompression()
    {
        return $this->getSetting('enable_http_compression');
    }

    /**
     * @param $flag
     *
     * @return $this
     */
    public function enableHttpCompression($flag)
    {
        $this->set('enable_http_compression', (int)$flag);

        return $this;
    }

    /**
     * @param bool $flag
     *
     * @return $this
     */
    public function readonly(bool $flag)
    {
        $this->set('readonly', $flag);
        return $this;
    }

    /**
     * @param $time
     *
     * @return $this
     */
    public function maxExecutionTime($time)
    {
        $this->set('max_execution_time', $time);

        return $this;
    }

    /**
     * @return array
     */
    public function getSettings()
    {
        return $this->settings;
    }

    /**
     * @param array $settingsArray
     *
     * @return self
     */
    public function apply(array $settingsArray)
    {
        foreach ($settingsArray as $key => $value) {
            $this->set($key, $value);
        }

        return $this;
    }

    /**
     * @param bool $flag
     */
    public function setReadOnlyUser(bool $flag)
    {
        $this->readOnlyUser = $flag;
    }

    /**
     * @return bool
     */
    public function isReadOnlyUser(): bool
    {
        return $this->readOnlyUser;
    }

    /**
     * @param string $name
     *
     * @return mixed|null
     */
    public function getSetting(string $name)
    {
        if (!isset($this->settings[$name])) {
            return null;
        }

        return $this->get($name);
    }
}