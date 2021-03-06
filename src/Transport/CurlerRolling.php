<?php

namespace Curler;

use ClickHouseDB\TransportException;

/**
 * Class CurlerRolling
 * @package Curler
 */
class CurlerRolling
{
    /**
     * @var int
     *
     * Max number of simultaneous requests.
     */
    private $simultaneousLimit = 10;

    /**
     * @var Request[]
     *
     * Requests currently being processed by curl
     */
    private $activeRequests = array();

    /**
     * @var int
     */
    private $runningRequests = 0;

    /**
     * @var Request[]
     *
     * Requests queued to be processed
     */
    private $pendingRequests = array();

    /**
     * @return int
     */
    private $completedRequestCount = 0;

    /**
     * @var null
     */
    private $_pool_master = null;

    /**
     * @var int
     */
    private $waitRequests = 0;

    /**
     * @var array
     */
    private $handleMapTasks = array();

    /**
     * @var string
     */
    private $_lashmakeQue_state = '';


    /**
     * CurlerRolling constructor.
     */
    public function __construct() {}

    /**
     *
     */
    public function __destructor()
    {
        $this->close();
    }


    /**
     * @return resource
     */
    private function handlerMulti()
    {
        if (!$this->_pool_master) {
            $this->_pool_master = curl_multi_init();

            if (function_exists('curl_multi_setopt')) {
                curl_multi_setopt($this->_pool_master, CURLMOPT_MAXCONNECTS, $this->simultaneousLimit);
            }
        }

        return $this->_pool_master;
    }

    /**
     *
     */
    public function close()
    {
        if ($this->_pool_master) {
            curl_multi_close($this->handlerMulti());
        }
    }


    /**
     * @param Request $req
     * @param bool $checkMultiAdd
     * @param bool $force
     * @return bool
     * @throws TransportException
     */
    public function addQueLoop(Request $req, $checkMultiAdd = true, $force = false)
    {
        $id = $req->getId();

        if (!$id) {
            $id = $req->getUniqHash($this->completedRequestCount);
        }

        if (!$force && isset($this->pendingRequests[$id])) {
            if (!$checkMultiAdd) {
                return false;
            }

            throw new TransportException("Cant add exists que - cant overwrite : $id!\n");
        }

        $this->pendingRequests[$id] = $req;
        return true;
    }

    /**
     * @param $oneHandle
     * @return Response
     */
    private function makeResponse($oneHandle)
    {
        $response = curl_multi_getcontent($oneHandle);
        $header_size = curl_getinfo($oneHandle, CURLINFO_HEADER_SIZE);
        $header = substr($response, 0, $header_size);
        $body = substr($response, $header_size);

        $n = new Response();
        $n->_headers = $this->parse_headers_from_curl_response($header);
        $n->_body = $body;
        $n->_info = curl_getinfo($oneHandle);
        $n->_error = curl_error($oneHandle);
        $n->_errorNo = curl_errno($oneHandle);
        $n->_useTime = 0;

        return $n;
    }

    /**
     * @param int $usleep
     * @return bool
     */
    public function execLoopWait($usleep = 10000)
    {
        // @todo rewrite wait
        $c = 0;

        // add all tasks
        do {
            $this->exec();

            $loop = $this->countActive();
            $c++;

            if ($c > 100000) {
                break;
            }

            usleep($usleep);
        } while ($loop);

        return true;
    }

    /**
     * @param $response
     * @return array
     */
    private function parse_headers_from_curl_response($response)
    {
        $headers = array();
        $header_text = $response;

        foreach (explode("\r\n", $header_text) as $i => $line) {
            if ($i === 0) {
                $headers['http_code'] = $line;
            }
            else {
                $r = explode(': ', $line);
                if (count($r) == 2) {
                    $headers[$r[0]] = $r[1];
                }
            }
        }

        return $headers;
    }

    /**
     * @return int
     */
    public function countPending()
    {
        return count($this->pendingRequests);
    }

    /**
     * @return int
     */
    public function countActive()
    {
        return count($this->activeRequests);
    }

    /**
     * @return int
     */
    public function countCompleted()
    {
        return $this->completedRequestCount;
    }

    /**
     * Set the limit for how many cURL requests will be execute simultaneously.
     *
     * Please be mindful that if you set this too high, requests are likely to fail
     * more frequently or automated software may perceive you as a DOS attack and
     * automatically block further requests.
     *
     * @param int $count
     * @throws \InvalidArgumentException
     * @return $this
     */
    public function setSimultaneousLimit($count)
    {
        if (!is_int($count) || $count < 2) {
            throw new \InvalidArgumentException("setSimultaneousLimit count must be an int >= 2");
        }

        $this->simultaneousLimit = $count;
        return $this;
    }

    /**
     * @return int
     */
    public function getSimultaneousLimit()
    {
        return $this->simultaneousLimit;
    }

    /**
     * @return int
     */
    public function getRunningRequests()
    {
        return $this->runningRequests;
    }

    /**
     * @param Request $req
     * @param bool $auto_close
     * @return mixed
     */
    public function execOne(Request $req, $auto_close = false)
    {
        $h = $req->handle();
        curl_exec($h);

        $req->setResponse($this->makeResponse($h));

        if ($auto_close) {
            $req->close();
        }

        return $req->response()->http_code();
    }

    /**
     * @return string
     */
    public function getInfo()
    {
        return "runningRequests = {$this->runningRequests} , pending=" . count($this->pendingRequests) . " ";
    }

    /**
     * @throws TransportException
     */
    public function exec()
    {
        $this->makePendingRequestsQue();

        // ensure we're running
        // a request was just completed -- find out which one

        while (($execrun = curl_multi_exec($this->handlerMulti(), $running)) == CURLM_CALL_MULTI_PERFORM);

        if ($execrun != CURLM_OK) {
            throw new TransportException("[ NOT CURLM_OK ]");
        }

        $this->runningRequests = $running;

        while ($done = curl_multi_info_read($this->handlerMulti())) {
            $response = $this->makeResponse($done['handle']);

            // send the return values to the callback function.

            $key = (string) $done['handle'];
            $task_id = $this->handleMapTasks[$key];
            $request = $this->pendingRequests[$this->handleMapTasks[$key]];

            unset($this->handleMapTasks[$key], $this->activeRequests[$task_id]);

            $this->pendingRequests[$task_id]->setResponse($response);
            $this->pendingRequests[$task_id]->onCallback();


            if (!$request->isPersistent()) {
                unset($this->pendingRequests[$task_id]);
            }

            $this->completedRequestCount++;

            // remove the curl handle that just completed
            curl_multi_remove_handle($this->handlerMulti(), $done['handle']);

            // if something was requeued, this will get it running/update our loop check values
            $status = curl_multi_exec($this->handlerMulti(), $active);
        }

        // see if there is anything to read
        curl_multi_select($this->handlerMulti(), 0.01);
        return $this->countActive();
    }

    /**
     *
     */
    public function makePendingRequestsQue()
    {
        $this->_lashmakeQue_state = "";

        $max = $this->getSimultaneousLimit();
        $active = $this->countActive();

        $this->_lashmakeQue_state .= "Active=$active | Max=$max |";

        if ($active < $max) {

            $canAdd = $max - $active;
            $pending = count($this->pendingRequests);

            $add = array();

            $this->_lashmakeQue_state .= " canAdd:$canAdd | pending=$pending |";

            foreach ($this->pendingRequests as $task_id => $params) {
                if (empty($this->activeRequests[$task_id])) {
                    $add[$task_id] = $task_id;
                    $this->_lashmakeQue_state .= '{A}';
                }
            }

            $this->_lashmakeQue_state .= ' sizeAdd=' . count($add);

            if (count($add)) {
                if ($canAdd >= count($add)) {
                    $ll = $add;
                }
                else {
                    $ll = array_rand($add, $canAdd);
                    if (!is_array($ll)) {
                        $ll = array($ll => $ll);
                    }
                }

                foreach ($ll as $task_id) {
                    $this->_prepareLoopQue($task_id);
                }

            }// if add
        }// if can add
    }

    /**
     * @param $task_id
     */
    private function _prepareLoopQue($task_id)
    {
        $this->activeRequests[$task_id] = 1;
        $this->waitRequests++;

        //
        $h = $this->pendingRequests[$task_id]->handle();

        // pool
        curl_multi_add_handle($this->handlerMulti(), $h);

        $key = (string) $h;
        $this->handleMapTasks[$key] = $task_id;
    }
}