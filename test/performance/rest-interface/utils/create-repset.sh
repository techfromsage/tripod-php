#!/bin/bash

#
# This script creates and starts a very basic Mongo replica set on the localhost
#
# Usage: create-repset.sh [-r repset-number] [-n number-of-nodes] [-p base-path-to-use-for-databases]
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
REPSET_CONF="/tmp/create-repset-$REPSET_NAME.js"
./start-repset.sh -r $REPSET_NUMBER -n $NODE_COUNT -p $DB_BASE_PATH -l $LOG_PATH

cat > $REPSET_CONF <<END-OF-CONF
rsconf = {
           _id: "$REPSET_NAME",
           members: [
                      {
                       _id: 0,
                       host: "127.0.0.1:$BASE_PORT"
                      }
                    ]
         }

rs.initiate( rsconf )
rs.conf()
END-OF-CONF

echo "Sleeping 15 seconds to bring up mongod services"
sleep 15

echo "Initiating replica set $REPSET_NAME"
mongo --port $BASE_PORT < $REPSET_CONF

echo "Sleeping 15 seconds to before adding to repset"
sleep 15
REPSET_NODE_SCRIPT="/tmp/add-node-to-repset-$REPSET_NAME.js"
cat > $REPSET_NODE_SCRIPT ""
COUNTER=1;
while [ $COUNTER -lt $NODE_COUNT ]; do
	: $((MONGO_PORT=$BASE_PORT+$COUNTER))
	: $((COUNTER=$COUNTER+1))
cat >> $REPSET_NODE_SCRIPT <<END-OF-CMD
rs.add("127.0.0.1:$MONGO_PORT")
END-OF-CMD
done

mongo --port $BASE_PORT < $REPSET_NODE_SCRIPT