FROM php:8.3-cli

RUN docker-php-ext-install pcntl sockets bcmath exif
RUN apt-get update && apt-get -y install openssl libcurl4-openssl-dev pkg-config libssl-dev libevent-dev

RUN pecl channel-update pecl.php.net && pecl install event-3.1.1 && docker-php-ext-enable --ini-name zz-event.ini event

COPY --from=composer /usr/bin/composer /usr/bin/composer

WORKDIR /var/www/clickhouse-queue
COPY . /var/www/clickhouse-queue/

RUN composer install

ENTRYPOINT ["php", "examples/run.php", "start"]
