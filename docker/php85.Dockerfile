FROM php:8.5.4-cli

RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  git \
  unzip \
  zip \
  && rm -rf /var/lib/apt/lists/*

RUN curl -sLo /tmp/mongosh.deb https://downloads.mongodb.com/compass/mongodb-mongosh_2.6.0_amd64.deb \
  && dpkg -i /tmp/mongosh.deb \
  && rm /tmp/mongosh.deb

COPY --from=mlocati/php-extension-installer:2 /usr/bin/install-php-extensions /usr/local/bin/
COPY --from=composer:2 /usr/bin/composer /usr/local/bin/

RUN IPE_ICU_EN_ONLY=1 install-php-extensions pcntl pcov xhprof mongodb-2.5.0
