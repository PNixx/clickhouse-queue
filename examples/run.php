<?php

use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Level;
use Monolog\Logger;
use PNixx\Clickhouse\ClickhouseWorker;
use Workerman\Worker;
use Workerman\Events\Fiber;

require __DIR__ . '/../vendor/autoload.php';

// Create the logger
$logger = new Logger('Worker');
$handler = new StreamHandler('php://output', Level::Warning);
$handler->setFormatter(new LineFormatter(null, null, true));
$logger->pushHandler($handler);

Worker::$eventLoopClass = Fiber::class;

$worker = new ClickhouseWorker([
	'clickhouse'    => array_filter([
		'database' => getenv('CLICKHOUSE_DB'),
		'host'     => getenv('CLICKHOUSE_HOST'),
		'port'     => getenv('CLICKHOUSE_PORT'),
		'user'     => getenv('CLICKHOUSE_USER'),
		'password' => getenv('CLICKHOUSE_PASSWORD'),
		'ssl'      => getenv('CLICKHOUSE_SSL') ?: null,
	]),
	'stomp'         => [
		'host'     => getenv('RABBIT_HOST'),
		'port'     => getenv('RABBIT_STOMP_PORT'),
		'user'     => getenv('RABBIT_USER'),
		'password' => getenv('RABBIT_PASSWORD'),
		'queue'    => getenv('RABBIT_QUEUE') ?: 'clickhouse',
		'vhost'    => getenv('RABBIT_VHOST') ?: '/',
	],
	'max_delay'     => getenv('MAX_DELAY'),
	'max_file_size' => getenv('MAX_FILE_SIZE'),
], __DIR__ . '/../tmp', $logger);

Worker::runAll();
