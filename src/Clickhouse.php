<?php

namespace PNixx\Clickhouse;

use Psr\Log\LoggerInterface;
use Workerman\Http\Client;

/**
 * Simple ClickHouse database adapter
 */
class Clickhouse {

	/**
	 * @var array{host: string, port: int, database: string, user: string, password: string}
	 */
	private array $config;

	private static Clickhouse $instance;
	private readonly Client $client;

	/**
	 * Tables schema
	 * @var array
	 */
	private array $schema = [];

	/**
	 * @return Clickhouse
	 */
	public static function get(): Clickhouse {
		return static::$instance;
	}

	/**
	 * Clickhouse constructor.
	 * @param array{host: string, port: int, database: string, user: string, password: string} $config
	 * @param float                                                                            $request_timeout
	 * @param LoggerInterface|null                                                             $logger
	 */
	public function __construct(array $config, float $request_timeout = 120, protected ?LoggerInterface &$logger = null) {
		$this->config = $config;
		$this->client = new Client(['timeout' => $request_timeout]);
		self::$instance = $this;
	}

	/**
	 * @param string $sql
	 * @return array|null
	 * @throws ClickhouseException
	 */
	public function execute(string $sql): ?array {
		$time = microtime(true);

		try {
			$result = $this->sql($sql);
		} finally {
			$this->log($time, $sql);
		}

		return $result['data'] ?? [];
	}

	/**
	 * The prepared data files are insert to the table.
	 * Must be a json list
	 * @param string $data
	 * @param string $table
	 * @throws ClickhouseException
	 */
	public function insert(string $data, string $table): void {
		$time = microtime(true);

		$params = [
			'database'                         => $this->config['database'],
			'max_partitions_per_insert_block'  => 0,
			'wait_end_of_query'                => 1,
			'send_progress_in_http_headers'    => 1,
			'session_timeout'                  => 600,
			'input_format_null_as_default'     => 1,
			'input_format_skip_unknown_fields' => 1,
			'date_time_input_format'           => 'best_effort',
			'query'                            => 'INSERT INTO ' . $table . ' FORMAT JSONEachRow ',
		];
		try {
			$this->request('http://' . $this->config['host'] . ':' . $this->config['port'] . '/?' . http_build_query($params), $data);
		} finally {
			$this->log($time, $params['query'] . '@-, data size: ' . round(strlen($data) / 1024, 2) . ' Kb');
		}
	}

	/**
	 * @param string $url
	 * @param string $body
	 * @return string
	 * @throws ClickhouseException
	 */
	private function request(string $url, string $body): string {
		$response = $this->client->request($url, [
			'method'  => 'POST',
			'headers' => ['Content-Type' => 'application/json'],
			'data'    => $body,
		]);
		if( $response->getStatusCode() >= 400 ) {
			throw new ClickhouseException($response->getBody(), $response->getStatusCode());
		}
		return $response->getBody();
	}

	/**
	 * @param string $sql
	 * @return array|null
	 * @throws ClickhouseException
	 */
	private function sql(string $sql): array|null {

		$params = array_filter([
			'database' => $this->config['database'],
			'user'     => $this->config['user'] ?? null,
			'password' => $this->config['password'] ?? null,
		]);
		$response = $this->request('http://' . $this->config['host'] . ':' . $this->config['port'] . '/?' . http_build_query($params), $sql . ' FORMAT JSON');
		return json_decode($response, true);
	}

	/**
	 * Fetch table schema
	 * @param $table
	 * @return array
	 * @throws ClickhouseException
	 */
	public function schema($table): array {
		if( empty($this->schema[$table]) ) {

			//Describe table info
			$data = $this->execute('DESCRIBE TABLE ' . $table);

			//Prepare table structure
			$this->schema[$table] = [];
			foreach( $data as $column ) {
				$this->schema[$table][$column['name']] = [
					'type'          => $this->columnConvertType($column['type']),
					'original_type' => strtolower($column['type']),
					'array'         => $this->columnIsArray($column['type']),
					'null'          => $this->columnIsNull($column['type']),
					'default'       => $column['default_expression'],
				];
			}
		}
		return $this->schema[$table];
	}

	/**
	 * Convert Clickhouse sql type to PHP type
	 * @param $type
	 * @return null|string
	 */
	private function columnConvertType($type): ?string {
		if( preg_match('/^(?:Array\()?(?:Nullable\()?(.*?)\)?\)?$/', $type, $match) ) {
			$type = $match[1];
		}
		return match ($type) {
			'Int8', 'Int32', 'Int64', 'UInt8', 'UInt32', 'UInt64' => 'int',
			'Float32', 'Float64' => 'float',
			'String', 'Date', 'DateTime' => 'string',
			default => null,
		};
	}

	/**
	 * Can a column be an NULL?
	 * @param $type
	 * @return bool
	 */
	private function columnIsNull($type): bool {
		return (bool)preg_match('/^Nullable\((.*?)\)$/', $type);
	}

	/**
	 * Can a column be an array?
	 * @param $type
	 * @return bool
	 */
	private function columnIsArray($type): bool {
		return (bool)preg_match('/^Array\((.*?)\)$/', $type);
	}

	/**
	 * Force convert value to column format
	 * @param string $table
	 * @param string $column
	 * @param mixed  $value
	 * @return mixed
	 * @throws ClickhouseException
	 */
	public function convertToType(string $table, string $column, mixed $value): mixed {
		$this->schema($table);

		//If there is no information about the column, clear the table data
		if( !isset($this->schema[$table][$column]) ) {
			$this->schema[$table] = null;
			$this->schema($table);
		}

		//Can be NULL
		if( $this->schema[$table][$column]['null'] && !$value && !in_array($value, ['', 0], true) ) {
			return null;
		}

		if( $this->schema[$table][$column]['array'] ) {
			if( !$value ) {
				$value = [];
			}
			foreach( $value as $v ) {
				//Fix incorrect type
				if( $this->schema[$table][$column]['type'] == 'string' && is_array($v) ) {
					$v = json_encode($v);
				} else {
					settype($v, $this->schema[$table][$column]['type']);
				}
			}
			return $value;
		} elseif( $this->schema[$table][$column]['original_type'] == 'date' && $value && !preg_match('/^\d{4}-\d{2}-\d{2}$/', $value) ) {
			$value = date('Y-m-d', strtotime($value));
		} elseif( $this->schema[$table][$column]['original_type'] == 'datetime' && $value && !preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $value) ) {
			$value = date('Y-m-d H:i:s', strtotime($value));
		} elseif( $this->schema[$table][$column]['type'] == 'string' && is_array($value) ) {
			$value = json_encode($value);
		} else {
			settype($value, $this->schema[$table][$column]['type']);
		}
		return $value;
	}

	/**
	 * @param float  $start_time
	 * @param string $sql
	 */
	private function log(float $start_time, string $sql): void {
		if( str_contains($sql, 'SELECT ') ) {
			$sql = "\033[1;34m{$sql}\033[0m";
		}
		if( str_contains($sql, 'INSERT ') ) {
			$sql = "\033[1;32m{$sql}\033[0m";
		}
		$this->logger?->debug("\033[1;36mClickhouse \033[1;35m(" . round((microtime(true) - $start_time) * 1000, 2) . "ms)\033[0m " . preg_replace('/\s\s+/', ' ', $sql));
	}
}

