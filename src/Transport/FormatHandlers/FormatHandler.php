<?php

namespace ClickHouseDB\Transport\FormatHandlers;

use Curler\Request;
use Curler\Response;

/**
 * Interface FormatHandler
 * @package ClickHouseDB\Transport\FormatHandlers
 */
interface FormatHandler
{
    /**
     * Get format name
     *
     * @return string
     */
    public function getName(): string;

    /**
     * @return Response
     */
    public function getResponse(): Response;

    /**
     * @param Response $response
     */
    public function setResponse(Response $response);

    /**
     * @return Request
     */
    public function getRequest():Request;

    /**
     * @param Request $request
     *
     * @return mixed
     */
    public function setRequest(Request $request);

    /**
     * @param $data
     *
     * @return string
     */
    public function getRequestParameters($data):string;
    /**
     * @return array
     */
    public function getResponseBodyData(): array;

    /**
     * @return array
     */
    public function getResponseBodyMeta(): array;

    /**
     * @return array
     */
    public function getResponseBodyTotals(): array;

    /**
     * @return array
     */
    public function getResponseBodyExtremes(): array;

    /**
     * @return int
     */
    public function getResponseBodyRows(): int;

    /**
     * @return int
     */
    public function getResponseBodyRowsBeforeLimitAtLeast(): int;

    /**
     * @return array
     */
    public function getResponseBodyStatistics(): array;

    /**
     * @return array
     */
    public function getResponseDataArray():array;
}