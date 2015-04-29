# development only.
echo "Use startWorkers.sh in development only. Suggest an upstart script for production."
QUEUE=tripod::development::apply,tripod::apply APP_INCLUDE=worker.inc.php php ../../vendor/chrisboulton/php-resque/bin/resque