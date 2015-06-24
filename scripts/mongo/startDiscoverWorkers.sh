#!/usr/bin/env bash
# development only.
echo "Use startDiscoverWorkers.sh in development only. Suggest an upstart script for production."
if [ -f "${BASH_SOURCE%/*}/../../vendor/chrisboulton/php-resque/bin/resque" ]
then
	PATH_TO_RESQUE_BIN="../.."
elif [ -f "${BASH_SOURCE%/*}/../../../../../vendor/chrisboulton/php-resque/bin/resque" ]
then
    PATH_TO_RESQUE_BIN="../../../../.."
else
	echo "php-resque not found."
	exit
fi
QUEUE=tripod::development::discover,tripod::discover APP_INCLUDE=${BASH_SOURCE%/*}/worker.inc.php php ${BASH_SOURCE%/*}/${PATH_TO_RESQUE_BIN}/vendor/chrisboulton/php-resque/bin/resque