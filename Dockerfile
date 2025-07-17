FROM php:8.4-cli

RUN docker-php-ext-install pcntl sockets bcmath exif
RUN apt-get update && apt-get -y install openssl libcurl4-openssl-dev pkg-config libssl-dev git zip

COPY --from=composer /usr/bin/composer /usr/bin/composer

WORKDIR /var/www/clickhouse-queue
COPY . /var/www/clickhouse-queue/

RUN /usr/bin/composer install

ENTRYPOINT ["php", "examples/run.php", "start"]
