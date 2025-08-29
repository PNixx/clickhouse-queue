<?php

namespace PNixx\Clickhouse\Test;

use PHPUnit\Framework\TestCase;
use PNixx\Clickhouse\Clickhouse;
use PNixx\Clickhouse\ClickhouseException;
use PNixx\Clickhouse\ClickhouseWorker;
use Revolt\EventLoop;
use Workerman\Events\Fiber;
use Workerman\Stomp\Client;
use Workerman\Timer;
use Workerman\Worker;

class ClickhouseWorkerTest extends TestCase {

	protected array $config = [
		'stomp'     => [
			'queue' => 'test-clickhouse-worker',
		],
		'max_delay' => 1,
	];
	const TMP = __DIR__ . '/../tmp';

	protected function setUp(): void {
		$this->config['stomp']['port'] = getenv('RABBIT_STOMP_PORT') ?: null;
		$this->config['clickhouse']['port'] = getenv('CLICKHOUSE_PORT') ?: null;
		if( !is_dir(self::TMP) ) {
			mkdir(self::TMP);
		}
		Worker::$globalEvent = new Fiber();
		Timer::init(Worker::$globalEvent);
	}

	public function testQueueProcessingOnStartup(): void {
		file_put_contents(self::TMP . '/test.json', json_encode(['column1' => '123']));
		$worker = new ClickhouseWorker($this->config, self::TMP);
		$worker->onWorkerStarted();
		$this->assertEmpty(array_diff(scandir(self::TMP), ['..', '.']));
	}

	public function testReceived(): void {
		$worker = $this->getMockBuilder(ClickhouseWorker::class)->setConstructorArgs([$this->config, self::TMP])->onlyMethods(['received'])->getMock();
		$worker->onWorkerStarted();
		$ref = new \ReflectionClass($worker);
		/** @var Client $client */
		$client = $ref->getProperty('client')->getValue($worker);

		EventLoop::defer(fn() => $client->send('/queue/' . $this->config['stomp']['queue'], '111'));

		$suspension = EventLoop::getSuspension();
		$timeout = EventLoop::delay(1, fn() => $suspension->throw(new \Exception('timeout wait body')));
		$worker->expects($this->once())->method('received')->willReturnCallback(function(Client $client, array $data) use ($suspension) {
			$suspension->resume($data['body'] ?? null);
			$client->ack($data['headers']['subscription'], $data['headers']['message-id']);
		});
		$this->assertEquals('111', $suspension->suspend());
		EventLoop::cancel($timeout);
	}

	public function testInsert(): void {
		$worker = new ClickhouseWorker($this->config, self::TMP);
		$worker->onWorkerStarted();

		try {
			Clickhouse::get()->execute('CREATE TABLE test (column1 String) ENGINE = MergeTree ORDER BY column1');

			$ref = new \ReflectionClass($worker);
			/** @var Client $client */
			$client = $ref->getProperty('client')->getValue($worker);

			$data = json_encode(['column1' => '123']);
			EventLoop::defer(fn() => $client->send('/queue/' . $this->config['stomp']['queue'], $data, ['table' => 'test']));

			$suspension = EventLoop::getSuspension();
			EventLoop::delay(1.2, fn() => $suspension->resume());
			$suspension->suspend();

			$this->assertCount(1, Clickhouse::get()->execute('SELECT * FROM test'));
		} finally {
			Clickhouse::get()->execute('DROP TABLE IF EXISTS test');
		}
	}

	public function testInvalidUserAndPassword() {
		$this->config['clickhouse']['user'] = 'fake' . uniqid();
		$this->config['clickhouse']['password'] = 'fake' . uniqid();

		$worker = new ClickhouseWorker($this->config, self::TMP);
		$worker->onWorkerStarted();

		$this->expectException(ClickhouseException::class);
		$this->expectExceptionMessageMatches('/DB::Exception:\s+' . $this->config['clickhouse']['user'] . ':\s+Authentication failed/');
		Clickhouse::get()->execute('SELECT 1');
	}
}
