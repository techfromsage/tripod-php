<?php

//todo: this file is mis-named. It has mongo specifics

if(!defined('TRIPOD_DIR')) define('TRIPOD_DIR', dirname(__FILE__) . '/' );

require_once TRIPOD_DIR.'../vendor/autoload.php';

// use latest brought in via composer
if(!defined('ARC_DIR')) define('ARC_DIR', TRIPOD_DIR. '../vendor/semsol/arc2/' );

//require_once ARC_DIR.'ARC2.php';

require_once TRIPOD_DIR.'classes/Timer.class.php';
require_once TRIPOD_DIR . 'exceptions/Exception.class.php';
require_once TRIPOD_DIR . 'exceptions/SearchException.class.php';
require_once TRIPOD_DIR . 'exceptions/CardinalityException.class.php';
require_once TRIPOD_DIR . 'exceptions/ConfigException.class.php';
require_once TRIPOD_DIR . 'exceptions/LabellerException.class.php';
require_once TRIPOD_DIR . 'exceptions/ViewException.class.php';
require_once TRIPOD_DIR.'mongo/MongoTripodConstants.php';
require_once TRIPOD_DIR.'mongo/MongoGraph.class.php';
require_once TRIPOD_DIR.'mongo/ImpactedSubject.class.php';
require_once TRIPOD_DIR . 'mongo/base/TripodBase.class.php';
require_once TRIPOD_DIR.'mongo/IComposite.php';
require_once TRIPOD_DIR.'mongo/base/CompositeBase.class.php';
require_once TRIPOD_DIR . 'mongo/delegates/TransactionLog.class.php';
require_once TRIPOD_DIR . 'mongo/delegates/Updates.class.php';
require_once TRIPOD_DIR . 'mongo/delegates/Views.class.php';
require_once TRIPOD_DIR . 'mongo/delegates/Tables.class.php';
require_once TRIPOD_DIR . 'mongo/delegates/SearchIndexer.class.php';
require_once TRIPOD_DIR.'ITripod.php';
require_once TRIPOD_DIR.'classes/ChangeSet.class.php';
require_once TRIPOD_DIR . '/mongo/Tripod.class.php';

require_once TRIPOD_DIR.'/mongo/base/JobBase.class.php';
require_once TRIPOD_DIR . '/mongo/jobs/DiscoverImpactedSubjects.class.php';
require_once TRIPOD_DIR.'/mongo/jobs/ApplyOperation.class.php';

Resque::setBackend(\Tripod\Mongo\Config::getResqueServer());

define('RDF_TYPE', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
define('RDF_SUBJECT', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#subject');
define('RDF_PREDICATE', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate');
define('RDF_OBJECT', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#object');
define('RDF_STATEMENT', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement');
define('RDF_FIRST', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#first');
define('RDF_REST', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#rest');
define('RDF_NIL', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#nil');
define('RDF_LIST', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#List');
define('RDF_VALUE', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#value');
define('RDF_', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_');

define('RDF_PROPERTY', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#Property');
define('RDF_XMLLITERAL', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral');

define('RDFS_LABEL', 'http://www.w3.org/2000/01/rdf-schema#label');
define('RDFS_COMMENT', 'http://www.w3.org/2000/01/rdf-schema#comment');
define('RDFS_ISDEFINEDBY', 'http://www.w3.org/2000/01/rdf-schema#isDefinedBy');
define('RDFS_DOMAIN', 'http://www.w3.org/2000/01/rdf-schema#domain');
define('RDFS_RANGE', 'http://www.w3.org/2000/01/rdf-schema#range');
define('RDFS_SEEALSO', 'http://www.w3.org/2000/01/rdf-schema#seeAlso');
define('RDFS_SUBPROPERTYOF', 'http://www.w3.org/2000/01/rdf-schema#subPropertyOf');
define('RDFS_SUBCLASSOF', 'http://www.w3.org/2000/01/rdf-schema#subClassOf');
define('RDFS_CLASS', 'http://www.w3.org/2000/01/rdf-schema#Class');
define('RDFS_DATATYPE', 'http://www.w3.org/2000/01/rdf-schema#Datatype');
define('RDFS_LITERAL', 'http://www.w3.org/2000/01/rdf-schema#Literal');
define('RDFS_RESOURCE', 'http://www.w3.org/2000/01/rdf-schema#Resource');
define('RDFS_MEMBER', 'http://www.w3.org/2000/01/rdf-schema#member');

define('FOAF_DOCUMENT', 'http://xmlns.com/foaf/0.1/Document');
define('FOAF_ISPRIMARYTOPICOF', 'http://xmlns.com/foaf/0.1/isPrimaryTopicOf');
define('FOAF_NICK', 'http://xmlns.com/foaf/0.1/nick');
define('FOAF_NAME', 'http://xmlns.com/foaf/0.1/name');
define('FOAF_PRIMARYTOPIC', 'http://xmlns.com/foaf/0.1/primaryTopic');
define('FOAF_TOPIC', 'http://xmlns.com/foaf/0.1/topic');
define('FOAF_PAGE', 'http://xmlns.com/foaf/0.1/page');

define('RSS_ITEM', 'http://purl.org/rss/1.0/item');
define('RSS_TITLE', 'http://purl.org/rss/1.0/title');
define('RSS_LINK', 'http://purl.org/rss/1.0/link');
define('RSS_DESCRIPTION', 'http://purl.org/rss/1.0/description');
define('RSS_ITEMS', 'http://purl.org/rss/1.0/items');

define('DC_TITLE', 'http://purl.org/dc/elements/1.1/title');
define('DC_DESCRIPTION', 'http://purl.org/dc/elements/1.1/description');
define('DC_CREATOR', 'http://purl.org/dc/elements/1.1/creator');
define('DC_DATE', 'http://purl.org/dc/elements/1.1/date');

define('DCT_ABSTRACT', 'http://purl.org/dc/terms/abstract');
define('DCT_PROVENANCE', 'http://purl.org/dc/terms/provenance');

define('OS_STARTINDEX','http://a9.com/-/spec/opensearch/1.1/startIndex');
define('OS_ITEMSPERPAGE','http://a9.com/-/spec/opensearch/1.1/itemsPerPage');
define('OS_TOTALRESULTS','http://a9.com/-/spec/opensearch/1.1/totalResults');

define('MIME_RDFXML', 'application/rdf+xml');
define('MIME_RSS', 'application/rss+xml');
define('MIME_XML', 'application/xml');
define('MIME_SPARQLRESULTS', 'application/sparql-results+xml');
define('MIME_FORMENCODED', 'application/x-www-form-urlencoded');

define('CS_SUBJECTOFCHANGE', 'http://purl.org/vocab/changeset/schema#subjectOfChange');
define('CS_CREATEDDATE', 'http://purl.org/vocab/changeset/schema#createdDate');
define('CS_CREATORNAME', 'http://purl.org/vocab/changeset/schema#creatorName');
define('CS_CHANGEREASON', 'http://purl.org/vocab/changeset/schema#changeReason');
define('CS_CHANGESET', 'http://purl.org/vocab/changeset/schema#ChangeSet');
define('CS_REMOVAL', 'http://purl.org/vocab/changeset/schema#removal');
define('CS_ADDITION', 'http://purl.org/vocab/changeset/schema#addition');

define('FRM_MAPPEDDATATYPEPROPERTY', 'http://schemas.talis.com/2006/frame/schema#mappedDatatypeProperty');
define('FRM_PROPERTY', 'http://schemas.talis.com/2006/frame/schema#property');
define('FRM_NAME', 'http://schemas.talis.com/2006/frame/schema#name');

define('BF_ANALYZER', 'http://schemas.talis.com/2006/bigfoot/configuration#analyzer');
define('BF_CREDENTIAL', 'http://schemas.talis.com/2006/bigfoot/configuration#credential');
define('BF_FIELDWEIGHT', 'http://schemas.talis.com/2006/bigfoot/configuration#fieldWeight');
define('BF_GROUP', 'http://schemas.talis.com/2006/bigfoot/configuration#group');
define('BF_GROUPREF', 'http://schemas.talis.com/2006/bigfoot/configuration#groupRef');
define('BF_JOBTYPE', 'http://schemas.talis.com/2006/bigfoot/configuration#jobType');
define('BF_MD5CREDENTIAL', 'http://schemas.talis.com/2006/bigfoot/configuration#md5credential');
define('BF_SNAPSHOTURI', 'http://schemas.talis.com/2006/bigfoot/configuration#snapshotUri');
define('BF_STARTTIME', 'http://schemas.talis.com/2006/bigfoot/configuration#startTime');
define('BF_STORE', 'http://schemas.talis.com/2006/bigfoot/configuration#store');
define('BF_STOREREF', 'http://schemas.talis.com/2006/bigfoot/configuration#storeRef');
define('BF_STORETEMPLATE', 'http://schemas.talis.com/2006/bigfoot/configuration#storeTemplate');
define('BF_WEIGHT', 'http://schemas.talis.com/2006/bigfoot/configuration#weight');

define('BF_STOREGROUP', 'http://schemas.talis.com/2006/bigfoot/configuration#StoreGroup');
define('BF_STOREGROUPREQUEST', 'http://schemas.talis.com/2006/bigfoot/configuration#StoreGroupRequest');
define('BF_JOBREQUEST', 'http://schemas.talis.com/2006/bigfoot/configuration#JobRequest');
define('BF_REINDEXJOB', 'http://schemas.talis.com/2006/bigfoot/configuration#ReindexJob');
define('BF_RESETDATAJOB', 'http://schemas.talis.com/2006/bigfoot/configuration#ResetDataJob');
define('BF_RESTOREJOB', 'http://schemas.talis.com/2006/bigfoot/configuration#RestoreJob');
define('BF_SNAPSHOTJOB', 'http://schemas.talis.com/2006/bigfoot/configuration#SnapshotJob');
define('BF_USER', 'http://schemas.talis.com/2006/bigfoot/configuration#User');


define('ANALYZER_STANDARDEN', 'http://schemas.talis.com/2007/bigfoot/analyzers#standard-en');
define('ANALYZER_STANDARDEL', 'http://schemas.talis.com/2007/bigfoot/analyzers#standard-el');
define('ANALYZER_STANDARDDE', 'http://schemas.talis.com/2007/bigfoot/analyzers#standard-de');
define('ANALYZER_STANDARDFR', 'http://schemas.talis.com/2007/bigfoot/analyzers#standard-fr');
define('ANALYZER_STANDARDCJK', 'http://schemas.talis.com/2007/bigfoot/analyzers#standard-cjk');
define('ANALYZER_STANDARDNL', 'http://schemas.talis.com/2007/bigfoot/analyzers#standard-nl');
define('ANALYZER_KEYWORD', 'http://schemas.talis.com/2007/bigfoot/analyzers#keyword');
define('ANALYZER_NOSTOPEN', 'http://schemas.talis.com/2007/bigfoot/analyzers#nostop-en');
define('ANALYZER_NORMEN', 'http://schemas.talis.com/2007/bigfoot/analyzers#norm-en');

define('OWL_CLASS', 'http://www.w3.org/2002/07/owl#Class');
define('OWL_COMPLEMENTOF', 'http://www.w3.org/2002/07/owl#complementOf');
define('OWL_EQUIVALENTCLASS', 'http://www.w3.org/2002/07/owl#equivalentClass');
define('OWL_EQUIVALENTPROPERTY', 'http://www.w3.org/2002/07/owl#equivalentProperty');
define('OWL_SAMEAS', 'http://www.w3.org/2002/07/owl#sameAs');
define('OWL_INVERSEOF', 'http://www.w3.org/2002/07/owl#inverseOf');
define('OWL_INVERSEFUNCTIONALPROPERTY', 'http://www.w3.org/2002/07/owl#InverseFunctionalProperty');
define('OWL_FUNCTIONALPROPERTY', 'http://www.w3.org/2002/07/owl#FunctionalProperty');
define('OWL_OBJECTPROPERTY', 'http://www.w3.org/2002/07/owl#ObjectProperty');
define('OWL_DATATYPEPROPERTY', 'http://www.w3.org/2002/07/owl#DatatypeProperty');
define('OWL_TRANSITIVEPROPERTY', 'http://www.w3.org/2002/07/owl#TransitiveProperty');
define('OWL_SYMMETRICPROPERTY', 'http://www.w3.org/2002/07/owl#SymmetricProperty');
define('OWL_ANNOTATIONPROPERTY', 'http://www.w3.org/2002/07/owl#AnnotationProperty');
define('OWL_DISJOINTWITH', 'http://www.w3.org/2002/07/owl#disjointWith');

define('OUTPUT_TYPE_RDF', 'rdf');
define('OUTPUT_TYPE_XML', 'xml');
define('OUTPUT_TYPE_TURTLE', 'turtle');
define('OUTPUT_TYPE_NTRIPLES', 'ntriples');
define('OUTPUT_TYPE_HTML', 'html');
define('OUTPUT_TYPE_JSON', 'json');
