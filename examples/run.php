<?php

use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Level;
use Monolog\Logger;
use PNixx\Clickhouse\ClickhouseWorker;
use Workerman\Worker;

require __DIR__ . '/../vendor/autoload.php';

// Create the logger
$logger = new Logger('Worker');
$handler = new StreamHandler('php://output', Level::Warning);
$handler->setFormatter(new LineFormatter(null, null, true));
$logger->pushHandler($handler);

$worker = new ClickhouseWorker([
	'clickhouse'    => [
		'database' => getenv('CLICKHOUSE_DB'),
		'host'     => getenv('CLICKHOUSE_HOST'),
		'port'     => getenv('CLICKHOUSE_PORT'),
	],
	'stomp'         => [
		'host'     => getenv('RABBIT_HOST'),
		'port'     => getenv('RABBIT_STOMP_PORT'),
		'user'     => getenv('RABBIT_USER'),
		'password' => getenv('RABBIT_PASSWORD'),
		'queue'    => getenv('RABBIT_QUEUE') ?: 'clickhouse',
	],
	'max_delay'     => getenv('MAX_DELAY'),
	'max_file_size' => getenv('MAX_FILE_SIZE'),
], __DIR__ . '/../tmp', $logger);

Worker::runAll();
