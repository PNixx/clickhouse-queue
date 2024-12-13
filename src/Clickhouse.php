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
	private readonly array $config;
	private readonly bool $ssl;

	private static Clickhouse $instance;
	private readonly Client $client;

	/**
	 * @return Clickhouse
	 */
	public static function get(): Clickhouse {
		return static::$instance;
	}

	/**
	 * Clickhouse constructor.
	 * @param array{host: string, port: int, database: string, user: ?string, password: ?string, ssl: ?bool} $config
	 * @param float                                                                                          $request_timeout
	 * @param LoggerInterface|null                                                                           $logger
	 */
	public function __construct(array $config, float $request_timeout = 120, protected ?LoggerInterface &$logger = null) {
		$this->ssl = $config['ssl'] ?? false;
		$this->config = array_merge(['database' => 'default', 'host' => '127.0.0.1', 'port' => 8123], array_diff_key(array_filter($config), array_flip(['ssl'])));
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
			$this->request($this->schema() . $this->config['host'] . ':' . $this->config['port'] . '/?' . http_build_query($params), $data);
		} finally {
			$this->log($time, $params['query'] . '@-, data size: ' . round(strlen($data) / 1024, 2) . ' Kb');
		}
	}

	/**
	 * @return string
	 */
	private function schema(): string {
		return $this->ssl ? 'https://' : 'http://';
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
		$response = $this->request($this->schema() . $this->config['host'] . ':' . $this->config['port'] . '/?' . http_build_query($params), $sql . ' FORMAT JSON');
		return json_decode($response, true);
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

