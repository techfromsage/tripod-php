#!/bin/bash

BASE_PORT=27017;
while getopts c:n:p: opt; do
	case "${opt}" in
		c)
			: $((CLUSTER_NUMBER=${OPTARG}-1))
			REPSET_NAME="rs"
			REPSET_NAME+=$CLUSTER_NUMBER
			: $((BASE_PORT_MODIFIER=$CLUSTER_NUMBER * 100))
			: $((BASE_PORT=$BASE_PORT + $BASE_PORT_MODIFIER))
			echo $REPSET_NAME
			;;
		n)
			NODE_COUNT=${OPTARG}
			;;
		p)
			DB_BASE_PATH=${OPTARG}
			;;
	esac
done 

COUNTER=0;
while [ $COUNTER -lt $NODE_COUNT ]; do
	: $((MONGO_PORT=$BASE_PORT+$COUNTER))
	: $((COUNTER=$COUNTER+1))
	echo $MONGO_PORT
done
