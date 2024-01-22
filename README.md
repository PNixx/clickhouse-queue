# Clickhouse Queue

Clickhouse insert buffer queue using RabbitMQ STOMP protocol.

## Required PHP Version

- PHP 8.1+

## Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require pnixx/clickhouse-queue
```

## Example usage

```php
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
```

This worker will listen the queue `clickhouse` and will inserting bulk data to ClickHouse database. For run worker:

```bash
php examples/run.php start
```

## Message structure

Header `table`: to insert data into the specified table

Body: JSON as string row data

```json
{"column1": "data1", "column2": "data2"}
```

## Donations

Donations to this project are going directly to [PNixx](https://github.com/PNixx), the original author of this project:

* BTC address: `1H3rhpf7WEF5JmMZ3PVFMQc7Hm29THgUfN`
* ETH address: `0x6F094365A70fe7836A633d2eE80A1FA9758234d5`
* XMR address: `42gP71qLB5M43RuDnrQ3vSJFFxis9Kw9VMURhpx9NLQRRwNvaZRjm2TFojAMC8Fk1BQhZNKyWhoyJSn5Ak9kppgZPjE17Zh`
* TON address: `UQBt0-s1igIpJoEup0B1yAUkZ56rzbpruuAjNhQ26MVCaNlC`

## Contributing

Bug reports and pull requests are welcome on GitHub at [https://github.com/PNixx/clickhouse-queue](https://github.com/PNixx/clickhouse-queue). This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.
