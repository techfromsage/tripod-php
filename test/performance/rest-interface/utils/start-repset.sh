#!/bin/bash
#
# This script starts a very basic Mongo replica set on the localhost
#
# Usage: start-repset.sh [-r repset-number] [-n number-of-nodes] [-p base-path-to-use-for-databases]
#
#   -r      The repset number is for running multiple repsets concurrently, the number is used for generating the repset
#           repset name ("rs3" for -r 3) and port (27017 + (repset number x 100)): defaults to 0
#   -n      Number of nodes to start in repset: defaults to 2
#   -p      Database path: defaults to /tmp/mongodb
#   -l      Path to log: defaults to /tmp

BASE_PORT=27017;
DB_BASE_PATH="/tmp/mongodb"
NODE_COUNT=2
REPSET_NUMBER=0
REPSET_NAME="rs$REPSET_NUMBER"
LOG_PATH="/tmp"

usage() { echo "Usage: $0 [-r <repset-number>] [-n <number-of-nodes>] [-p </base/path/to/database>] [-l </path/to/logs>" 1>&2; exit 0; }

while getopts r:n:p:l: opt; do
	case "${opt}" in
		r)
			REPSET_NUMBER=${OPTARG}
			REPSET_NAME="rs"
			REPSET_NAME+=$REPSET_NUMBER
			: $((BASE_PORT_MODIFIER=$REPSET_NUMBER * 100))
			: $((BASE_PORT=$BASE_PORT + $BASE_PORT_MODIFIER))
			;;
		n)
			NODE_COUNT=${OPTARG}
			;;
		p)
			DB_BASE_PATH=${OPTARG}
			;;
		l)
		    LOG_PATH=${OPTARG}
		    ;;
        *)
            usage
            ;;
	esac
done 

COUNTER=0;
while [ $COUNTER -lt $NODE_COUNT ]; do
	: $((MONGO_PORT=$BASE_PORT+$COUNTER))
	CURR_PATH=$DB_BASE_PATH/rs$REPSET_NUMBER-$COUNTER
	echo "Creating $CURR_PATH, if it does not exist"
	mkdir -p $CURR_PATH
    echo "Starting Mongo node $COUNTER on port $MONGO_PORT"
	nohup mongod --port $MONGO_PORT --dbpath $CURR_PATH --replSet $REPSET_NAME --smallfiles --oplogSize 128 >> $LOG_PATH/mongod-$REPSET_NAME-$COUNTER.log &
	: $((COUNTER=$COUNTER+1))
done
