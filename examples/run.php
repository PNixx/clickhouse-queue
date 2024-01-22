<?php

use PNixx\Clickhouse\ClickhouseWorker;
use Workerman\Worker;

require __DIR__ . '/../vendor/autoload.php';

$worker = new ClickhouseWorker([
	'clickhouse' => [
		'database' => 'example',
		'host'     => 'localhost',
		'port'     => 8123,
	],
	'stomp'      => [
		'host'  => 'localhost',
		'port'  => 61613,
		'queue' => 'clickhouse',
	],
], __DIR__ . '/../tmp');

Worker::runAll();
