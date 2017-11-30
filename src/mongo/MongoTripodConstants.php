<?php

// Collection names
define('TTL_CACHE_COLLECTION', 'ttl_cache');
define('TABLE_ROWS_COLLECTION', 'table_rows');
define('VIEWS_COLLECTION', 'views');
define('LOCKS_COLLECTION', 'locks');
define('AUDIT_MANUAL_ROLLBACKS_COLLECTION','audit_manual_rollbacks');

// search
define('SEARCH_INDEX_COLLECTION', 'search');
define("SEARCH_PROVIDER_MONGO", '\Tripod\Mongo\MongoSearchProvider');
define("SEARCH_PROVIDER_ELASTIC_SEARCH", 'es');

// Tripod document properties
define('_ID_KEY', '_id');
define('_ID_RESOURCE','r');
define('_ID_CONTEXT','c');
define('_ID_TYPE','type');
define('_VERSION','_version');
define('_LOCKED_FOR_TRANS','_lockedForTrans');
define('_LOCKED_FOR_TRANS_TS','_lts');
define('_IMPACT_INDEX','_impactIndex');
define('_GRAPHS','_graphs');
define('_EXPIRES','_expires');
define('VALUE_URI','u');
define('VALUE_LITERAL','l');
define('_UPDATED_TS', '_uts');
define('_CREATED_TS', '_cts');

// operations that Tripod performs
define('OP_VIEWS','generate_views');
define('OP_TABLES','generate_table_rows');
define('OP_SEARCH','generate_search_index_docs');
define('OP_SYNC', 'sync');
define('OP_ASYNC', 'async');
define('OP_QUEUE', 'queue');

// operands provided by mongo
define("MONGO_OPERATION_SET",'$set');
define("MONGO_OPERATION_INC",'$inc');
define("MONGO_OPERATION_UNSET",'$unset');
define("MONGO_OPERATION_GTE",'$gte');
define("MONGO_OPERATION_LTE",'$lte');

// query types, used mainly for logging
define('MONGO_SELECT','MONGO_SELECT');
define('MONGO_DESCRIBE','MONGO_DESCRIBE');
define('MONGO_GET_GRAPH','MONGO_GET_GRAPH');
define('MONGO_DESCRIBE_WITH_CONDITION','MONGO_DESCRIBE_WITH_CONDITION');
define('MONGO_MULTIDESCRIBE','MONGO_MULTIDESCRIBE');
define('MONGO_VIEW','MONGO_VIEW');
define('MONGO_VIEW_CACHE_MISS','MONGO_VIEW_CACHE_MISS');
define('MONGO_TABLE_ROWS','MONGO_TABLE_ROWS');
define('MONGO_TABLE_ROWS_DISTINCT','MONGO_TABLE_ROWS_DISTINCT');
define('MONGO_GROUP','MONGO_GROUP');
define('MONGO_COUNT','MONGO_COUNT');
define('MONGO_GET_ETAG','MONGO_GET_ETAG');

// tripod index names, these are the names for specific indexes we HAVE to create for tripod to work optimally
define('_LOCKED_FOR_TRANS_INDEX','_lockedForTransIdx');
define('_UPDATED_TS_INDEX','_updatedTsIdx');
define('_CREATED_TS_INDEX','_createdTsIdx');

// other events, used mainly for logging
define('MONGO_IMPACT_INDEX_HIT','MONGO_IMPACT_INDEX_HIT');
define('MONGO_CREATE_VIEW','MONGO_CREATE_VIEW');
define('MONGO_CREATE_TABLE','MONGO_CREATE_TABLE');
define('MONGO_DELETE_TABLE_ROWS','MONGO_DELETE_TABLE_ROWS');
define('MONGO_FIND_IMPACTED','MONGO_FIND_IMPACTED');
define('MONGO_ON_THE_FLY_MR','MONGO_ON_THE_FLY_MR');
define('MONGO_WRITE','MONGO_WRITE');
define('MONGO_ROLLBACK','MONGO_ROLLBACK');
define('MONGO_LOCK','MONGO_LOCK');
define('MONGO_ADD_TO_QUEUE','MONGO_ADD_TO_QUEUE');

define('MONGO_QUEUE_DISCOVER_JOB','MONGO_QUEUE_DISCOVER_JOB');
define('MONGO_QUEUE_DISCOVER_SUBJECT','MONGO_QUEUE_DISCOVER_SUBJECT');
define('MONGO_QUEUE_DISCOVER_SUCCESS','MONGO_QUEUE_DISCOVER_SUCCESS');
define('MONGO_QUEUE_DISCOVER_FAIL','MONGO_QUEUE_DISCOVER_FAIL');

define('MONGO_QUEUE_APPLY_OPERATION','MONGO_QUEUE_APPLY_OPERATION');
define('MONGO_QUEUE_APPLY_OPERATION_JOB','MONGO_QUEUE_APPLY_OPERATION_JOB');
define('MONGO_QUEUE_APPLY_OPERATION_SUCCESS','MONGO_QUEUE_APPLY_OPERATION_SUCCESS');
define('MONGO_QUEUE_APPLY_OPERATION_FAIL','MONGO_QUEUE_APPLY_OPERATION_FAIL');

define('MONGO_QUEUE_ENSURE_INDEXES','MONGO_QUEUE_ENSURE_INDEXES');
define('MONGO_QUEUE_ENSURE_INDEXES_JOB','MONGO_QUEUE_ENSURE_INDEXES_JOB');
define('MONGO_QUEUE_ENSURE_INDEXES_SUCCESS','MONGO_QUEUE_ENSURE_INDEXES_SUCCESS');
define('MONGO_QUEUE_ENSURE_INDEXES_FAIL','MONGO_QUEUE_ENSURE_INDEXES_FAIL');

define('MONGO_CREATE_SEARCH_DOC','MONGO_CREATE_SEARCH_DOC');
define('MONGO_CONNECTION_ERROR','MONGO_CONNECTION_ERROR');

define('SUBJECT_COUNT', 'subject_count');

define('STAT_CLASS', 'tripod');
define('STAT_PIVOT_FIELD', 'group_by_db');

define('BATCH_TRACKING_GROUP', 'BATCH_TRACKING_GROUP');

//Audit types, statuses
define('AUDIT_TYPE_REMOVE_INERT_LOCKS', 'REMOVE_INERT_LOCKS');
define('AUDIT_STATUS_IN_PROGRESS', 'IN_PROGRESS');
define('AUDIT_STATUS_COMPLETED', 'COMPLETED');
define('AUDIT_STATUS_ERROR', 'ERROR');

// Mongo constants
define('DEFAULT_MONGO_CONNECT_TIMEOUT_MS', 20000);

// queue constants
define("RESQUE_SERVER","RESQUE_SERVER");
define("MONGO_TRIPOD_RESQUE_SERVER","MONGO_TRIPOD_RESQUE_SERVER"); // deprecated, will be removed in future releases
define("TRIPOD_DISCOVER_QUEUE","TRIPOD_DISCOVER_QUEUE");
define("TRIPOD_APPLY_QUEUE","TRIPOD_APPLY_QUEUE");
define("TRIPOD_ENSURE_INDEXES_QUEUE","TRIPOD_ENSURE_INDEXES_QUEUE");

// View functions
define("INCLUDE_RDF_SEQUENCE","_seq_");
