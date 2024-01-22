<?php
namespace PNixx\Clickhouse;

use Psr\Log\LoggerInterface;
use Revolt\EventLoop;
use Workerman\Stomp\Client;
use Workerman\Worker;

class ClickhouseWorker extends Worker {

	//Max buffer file size for export to clickhouse
	public static int $max_file_size = 5242880;
	//Max buffer delay in seconds for export to clickhouse
	public static int $max_delay = 20;

	protected Client $client;

	/**
	 * @var array{
	 *    clickhouse: array{host: string, port: int, database: string, user: string, password: string},
	 *    stomp: array{host: string, port: int, queue: string, user: string, password: string}
	 *  }
	 */
	protected readonly array $config;

	//Unix process name
	protected string $process_name;

	//Buffer variables
	private array $lock = [true => false, false => false];
	private bool $started = false;
	private array $queue_time = [];
	private string $loop_queue;
	private string $loop_bad;
	private array $fp = [];

	/**
	 * @param array{
	 *   clickhouse: array{host: string, port: int, database: string, user: string, password: string},
	 *   stomp: array{host: string, port: int, queue: string, user: string, password: string}
	 * }                           $config
	 * @param string               $tmp_directory
	 * @param LoggerInterface|null $logger
	 */
	public function __construct(array $config, protected readonly string $tmp_directory, protected ?LoggerInterface $logger = null) {
		parent::__construct();
		$this->process_name = 'clickhouse-queue';
		$this->onWorkerStart = $this->onWorkerStarted(...);
		$this->onWorkerStop = $this->onStop(...);
		$this->config = $config;
		if( !is_dir($this->tmp_directory) ) {
			mkdir($this->tmp_directory);
		}
		if( empty($this->config['stomp']['queue']) ) {
			exit('STOMP queue missing');
		}
	}

	/**
	 * Main working
	 * @return void
	 * @throws ClickhouseException
	 */
	public function onWorkerStarted(): void {
		$this->logger?->info('Starting');
		if( $this->process_name ) {
			@cli_set_process_title($this->process_name);
		}

		//Only works in UTC
		date_default_timezone_set('UTC');

		//Catch unhandled errors
		EventLoop::setErrorHandler(function(\Throwable $e): void {
			$this->logger?->error('Exception handler: ' . get_class($e) . ', ' . $e->getMessage() . ', ' . $e->getFile() . ':' . $e->getLine() . PHP_EOL . $e->getTraceAsString());
		});

		//New STOMP connection
		$this->stompConnect();

		//Initialize Clickhouse
		new Clickhouse($this->config['clickhouse'], 120, $this->logger);

		//Get information all tables
		$tables = array_filter(array_column(Clickhouse::get()->execute('SHOW TABLES'), 'name'), fn($v) => !str_starts_with($v, '.'));
		foreach( $tables as $table ) {
			Clickhouse::get()->schema($table);
		}

		//Start queue updating
		$this->queue();
		EventLoop::defer(fn() => $this->queue(true));
		$this->logger?->info('Queue cleared');
		$this->started = true;

		//Subscribe to STOMP channel
		$this->client->subscribe('/queue/' . $this->config['stomp']['queue'], $this->received(...), [
			'id'             => gethostname() . '-' . getmypid(),
			'ack'            => 'client-individual',
			'durable'        => 'true',
			'prefetch-count' => 10,
		]);

		//We start processing queues every N seconds
		$this->loop_queue = EventLoop::repeat(self::$max_delay, fn() => $this->queue());
		//We check for broken files once an hour
		$this->loop_bad = EventLoop::repeat(3600, fn() => $this->queue(true));
	}

	/**
	 * Requested stop event
	 */
	public function onStop(): void {
		if( isset($this->loop_queue) ) {
			EventLoop::cancel($this->loop_queue);
		}
		if( isset($this->loop_bad) ) {
			EventLoop::cancel($this->loop_bad);
		}
	}

	/**
	 * Received a new message
	 * @param Client $client
	 * @param array{
	 *   cmd: string,
	 *   headers: array{subscription: string, destination: string, message-id: string, redelivered: bool, table: ?string, content-type: string, content-length: string},
	 *   body: string
	 * }             $data
	 */
	public function received(Client $client, array $data): void {

		$this->logger->debug('Received', $data);
		//New option when the table is sent in headers
		//At the same time, it would be nice if the data format was correct, then you won’t have to decode and encode json.
		//Because Clickhouse does not allow you to insert a number into the string column.
		if( isset($data['headers']['table']) ) {
			$table = $data['headers']['table'];
			$values = json_decode($data['body'], true);
		} else {
			//Old version
			$body = json_decode($data['body'], true);
			$table = $body['table'] ?? null;
			$values = $body['values'] ?? null;
		}
		try {
			if( empty($table) ) {
				$this->logger?->error('Table name missing');
			} else {
				//Format data types
				foreach( $values as $column => $value ) {
					$values[$column] = Clickhouse::get()->convertToType($table, $column, $value);
				}

				//Append row
				if( empty($this->fp[$table]) ) {
					$this->fp[$table] = fopen($this->path($table), 'a+');
					stream_set_blocking($this->fp[$table], false);
				}
				//Lock file for write
				if( flock($this->fp[$table], LOCK_EX) ) {
					fwrite($this->fp[$table], json_encode($values, JSON_UNESCAPED_UNICODE) . PHP_EOL);
				}
				flock($this->fp[$table], LOCK_UN);

				//Set create buffer time
				if( !isset($this->queue_time[$table]) ) {
					$this->queue_time[$table] = time();
				}
			}

			//Ack
			$client->ack($data['headers']['subscription'], $data['headers']['message-id']);
		} catch (\Throwable $e) {
			$this->logger?->error(get_class($e) . ', ' . $e->getMessage() . ', table: ' . $table . ', message: ' . json_encode($values, JSON_UNESCAPED_UNICODE) . PHP_EOL . $e->getTraceAsString());
			$suspension = EventLoop::getSuspension();
			EventLoop::delay(10, fn() => $suspension->resume());
			$suspension->suspend();
			try {
				$client->nack($data['headers']['subscription'], $data['headers']['message-id']);
			} catch (\Throwable $e) {
				$this->logger?->warning($e::class . ':' . $e->getMessage());
			}
		}
	}

	/**
	 * Processes internal queue to insert data in batches
	 * @param bool $force Need processing of dropped files
	 */
	public function queue(bool $force = false): void {
		if( !$this->lock[$force] ) {
			try {
				$this->lock[$force] = true;
				//Each in directory buffer
				foreach( glob($this->tmp_directory . '/*.json' . ($force ? '.*' : '')) as $filename ) {
					$table = pathinfo(preg_replace('/\.json(\..*?)?$/', '.json', $filename), PATHINFO_FILENAME);
					if( $force || $this->availableForProcessing($table) ) {
						$this->processing($filename, $table);
					}
				}
			} finally {
				$this->lock[$force] = false;
			}
		}
	}

	/**
	 * @param string $table
	 * @return string
	 */
	protected function path(string $table): string {
		return $this->tmp_directory . '/' . $table . '.json';
	}

	/**
	 * Checks if the queue is ready to be inserted into the table
	 * @param $table
	 * @return bool
	 */
	protected function availableForProcessing($table): bool {
		if( !$this->started ) {
			return true;
		}

		if( !isset($this->queue_time[$table]) ) {
			return false;
		}

		//If there was no insertion and the last time the data arrived more than N seconds ago
		if( isset($this->queue_time[$table]) && $this->queue_time[$table] <= strtotime('-' . self::$max_delay . ' seconds') ) {
			return true;
		}

		return filesize($this->path($table)) >= self::$max_file_size;
	}

	/**
	 * Starts buffer processing for the specified table
	 * @param string $file
	 * @param string $table
	 */
	protected function processing(string $file, string $table): void {

		//We copy the data so that during processing it is not added accidentally
		if( str_ends_with($file, '.json') ) {

			//Close the file assess and remove it from the array
			if( isset($this->fp[$table]) && flock($this->fp[$table], LOCK_EX) ) {
				fclose($this->fp[$table]);
				unset($this->fp[$table]);
			}

			//Rename file
			$path = $file . '.' . uniqid();
			rename($file, $path);
			unset($this->queue_time[$table]);
		} else {
			$path = $file;
		}

		//Upload data to Clickhouse
		try {
			$fp = fopen($path, 'r');
			stream_set_blocking($fp, false);
			$data = stream_get_contents($fp);
			fclose($fp);
			Clickhouse::get()->insert($data, $table);
			unlink($path);
		} catch (\Throwable $e) {
			$this->logger?->error(get_class($e) . ', ' . $e->getMessage(), array_slice($e->getTrace(), 0, 2));
		}
	}

	/**
	 * Initialize STOMP connection
	 * https://www.rabbitmq.com/stomp.html
	 * @return void
	 */
	private function stompConnect(): void {
		$this->client = new Client('stomp://' . $this->config['stomp']['host'] . ':' . $this->config['stomp']['port'], array_filter([
			'login'            => $this->config['stomp']['user'] ?? null,
			'passcode'         => $this->config['stomp']['password'] ?? null,
			'reconnect_period' => 1,
		]));

		//Wait connection success
		$suspension = EventLoop::getSuspension();
		$this->client->onConnect = function() use ($suspension) {
			$this->logger?->info('STOMP connected');
			$suspension->resume();
		};
		$this->client->onError = function(\Exception $e) {
			$this->logger?->warning('STOMP: ' . $e::class . ', ' . $e->getMessage());
		};

		//Connecting
		$this->client->connect();
		$suspension->suspend();
	}
}
