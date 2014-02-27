#!/bin/bash

echo "************************"
echo "Doing clean up for old log files"

sudo rm -f /tmp/tripod-locks.log

echo "************************"
echo "Running Locks Test"
echo "************************"
echo ""

ab -c 50 -n 1500 http://locks/testMultiDocUpdate.php

echo "Locks Tests Complete"
echo "************************"
echo ""

echo "Running Locks log filter to get only log entries, we are interested in"
echo "************************"
echo ""
grep 'TRIPOD_ERR' /tmp/tripod-locks.log  > locks.log

echo "Log Filter complete, final log is at ./locks.log"
echo "************************"
echo ""

exit 0