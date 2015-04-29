# development only.
echo "Use startWorkers.sh in development only. Suggest an upstart script for production."
QUEUE=tripod::development::apply,tripod::development::discover,tripod::apply,tripod::discover APP_INCLUDE=worker.inc.php php ../../vendor/chrisboulton/php-resque/bin/resque