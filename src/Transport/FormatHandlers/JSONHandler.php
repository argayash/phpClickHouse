<?php

namespace ClickHouseDB\Transport\FormatHandlers;


use Curler\Request;
use Curler\Response;

/**
 * Class JSONHandler
 * {@inheritdoc}
 * @package ClickHouseDB\Transport\FormatHandlers
 */
class JSONHandler implements FormatHandler
{
    const FORMAT_NAME = 'JSON';

    /** @var  Response */
    protected $response;

    /** @var  Request */
    protected $request;

    /**
     * @var array
     */
    protected $bodyDataArray;

    /**
     * {@inheritdoc}
     *
     * @return string
     */
    public function getName(): string
    {
        return self::FORMAT_NAME;
    }

    /**
     * @param $data
     *
     * @return string
     * @throws \ClickHouseDB\TransportException
     */
    public function getRequestParameters($data): string
    {
        $parameters = null;

        $this->getRequest()->header("Content-Type", "application/json, text/javascript; charset=utf-8");
        $this->getRequest()->header("Accept", "application/json, text/javascript, */*; q=0.01");

        if ($data === null) {
            $parameters = '{}';
        } elseif (is_array($data)) {
            $parameters = json_encode($data);
        } elseif (is_string($data)) {
            $parameters = $data;
        }

        if (!$parameters && $data) {
            throw new \ClickHouseDB\TransportException('Cant json_encode: ' . $data);
        }

        return $parameters;
    }

    /**
     * @return Response
     */
    public function getResponse(): Response
    {
        return $this->response;
    }

    /**
     * @param Response $response
     */
    public function setResponse(Response $response)
    {
        $this->response = $response;
    }

    /**
     * @return Request
     */
    public function getRequest(): Request
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
     * @return array
     */
    public function getResponseBodyData(): array
    {
        return $this->getResponseBodyData()['data']??[];
    }

    /**
     * @return array
     */
    public function getResponseBodyMeta(): array
    {
        return $this->getResponseBodyData()['meta']??[];
    }

    /**
     * @return array
     */
    public function getResponseBodyTotals(): array
    {
        return $this->getResponseBodyData()['totals']??[];
    }

    /**
     * @return array
     */
    public function getResponseBodyExtremes(): array
    {
        return $this->getResponseBodyData()['extremes']??[];
    }

    /**
     * @return int
     */
    public function getResponseBodyRows(): int
    {
        return $this->getResponseBodyData()['rows']??0;
    }

    /**
     * @return int
     */
    public function getResponseBodyRowsBeforeLimitAtLeast(): int
    {
        return $this->getResponseBodyData()['rows_before_limit_at_least']??0;
    }

    /**
     * @return array
     */
    public function getResponseBodyStatistics(): array
    {
        return $this->getResponseBodyData()['statistic']??[];
    }

    /**
     * @return array
     */
    public function getResponseDataArray(): array
    {
        if (null === $this->bodyDataArray) {
            $this->bodyDataArray = json_decode($this->getResponse()->body(), true);
        }

        return $this->bodyDataArray;
    }
}