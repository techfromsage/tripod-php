<?php

require_once 'MongoTripodTestBase.php';
/**
 * Class DiscoverImpactedSubjectsTest
 */
class DiscoverImpactedSubjectsTest extends MongoTripodTestBase
{
    protected $args = array();
    public function testMandatoryArgTripodConfig()
    {
        $this->setArgs();
        unset($this->args['tripodConfig']);
        $job = new \Tripod\Mongo\Jobs\DiscoverImpactedSubjects();
        $job->args = $this->args;
        $job->job->payload['id'] = uniqid();
        $this->setExpectedException('Exception', "Argument tripodConfig was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgStoreName()
    {
        $this->setArgs();
        unset($this->args['storeName']);
        $job = new \Tripod\Mongo\Jobs\DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument storeName was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgPodName()
    {
        $this->setArgs();
        unset($this->args['podName']);
        $job = new \Tripod\Mongo\Jobs\DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument podName was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgChanges()
    {
        $this->setArgs();
        unset($this->args['changes']);
        $job = new \Tripod\Mongo\Jobs\DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument changes was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgOperations()
    {
        $this->setArgs();
        unset($this->args['operations']);
        $job = new \Tripod\Mongo\Jobs\DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument operations was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgContextAlias()
    {
        $this->setArgs();
        unset($this->args['contextAlias']);
        $job = new \Tripod\Mongo\Jobs\DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument contextAlias was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testSubmitApplyOperationsJob()
    {
        $this->setArgs();

        $this->args['statsConfig'] = $this->getStatsDConfig();

        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $views = $this->getMockBuilder('\Tripod\Mongo\Composites\Views')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(
                array(
                    'tripod_php_testing',
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/'
                )
            )->getMock();

        $tables = $this->getMockBuilder('\Tripod\Mongo\Composites\Tables')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(
                array(
                    'tripod_php_testing',
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/'
                )
            )->getMock();

        $search = $this->getMockBuilder('\Tripod\Mongo\Composites\SearchIndexer')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $tripod->expects($this->exactly(3))
            ->method('getComposite')
            ->will($this->returnValueMap(
                array(
                    array(OP_VIEWS, $views),
                    array(OP_TABLES, $tables),
                    array(OP_SEARCH, $search)
                )
            ));

        $discoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('getTripod', 'getApplyOperation', 'getStat'))
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            array('timer','increment')
        );

        $discoverImpactedSubjects->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $discoverImpactedSubjects->args = $this->args;

        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('createJob'))
            ->getMock();

        $viewSubject = new \Tripod\Mongo\ImpactedSubject(
            array(
                _ID_RESOURCE=>'http://example.com/resources/foo',
                _ID_CONTEXT=>$this->args['contextAlias']
            ),
            OP_VIEWS,
            $this->args['storeName'],
            $this->args['podName']
        );

        $views->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(
                array($viewSubject)
            ));

        $tableSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>'http://example.com/resources/foo2',
                    _ID_CONTEXT=>$this->args['contextAlias']
                ),
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                array('t_foo_bar')
            ),
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>'http://example.com/resources/foo3',
                    _ID_CONTEXT=>$this->args['contextAlias']
                ),
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                array()
            )
        );

        $tables->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(
                $tableSubjects
            ));

        $search->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(array()));

        $discoverImpactedSubjects->expects($this->exactly(2))
            ->method('getApplyOperation')
            ->will($this->returnValue($applyOperation));

        $discoverImpactedSubjects->expects($this->exactly(5))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $applyOperation->expects($this->exactly(2))
            ->method('createJob')
            ->withConsecutive(
                array(
                    array($viewSubject),
                    \Tripod\Mongo\Config::getApplyQueueName(),
                    array('statsConfig'=>$this->args['statsConfig'])
                ),
                array(
                   $tableSubjects,
                    \Tripod\Mongo\Config::getApplyQueueName(),
                    array('statsConfig'=>$this->args['statsConfig'])
                )
            );

        $statMock->expects($this->exactly(4))
            ->method('timer')
            ->withConsecutive(
                array(MONGO_QUEUE_DISCOVER_SUBJECT, $this->anything()),
                array(MONGO_QUEUE_DISCOVER_SUBJECT, $this->anything()),
                array(MONGO_QUEUE_DISCOVER_SUBJECT, $this->anything()),
                array(MONGO_QUEUE_DISCOVER_SUCCESS, $this->anything())
            );

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_DISCOVER_JOB . '.' . SUBJECT_COUNT, 3);

        $discoverImpactedSubjects->perform();
    }

    public function testCreateJobDefaultQueue()
    {
        $labeller = new \Tripod\Mongo\Labeller();

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias('http://example.com/1')=>array('rdf:type','spec:name'),
            $labeller->uri_to_alias('http://example.com/2')=>array('rdf:type','dct:title','dct:subject')
        );

        $jobData = array(
            'changes'=>$subjectsAndPredicatesOfChange,
            'operations'=>array(OP_SEARCH),
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
            'storeName'=>'tripod_php_testing',
            'podName'=>'CBD_testing',
            'contextAlias'=>'http://talisaspire.com/'
        );

        /** @var \Tripod\Mongo\Jobs\DiscoverImpactedSubjects|PHPUnit_Framework_MockObject_MockObject $discoverImpactedSubjects */
        $discoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('submitJob'))
            ->setMockClassName('MockDiscoverImpactedSubjects')
            ->getMock();

        $discoverImpactedSubjects->expects($this->once())
            ->method('submitJob')
            ->with(
                \Tripod\Mongo\Config::getDiscoverQueueName(),
                'MockDiscoverImpactedSubjects',
                $jobData
            );

        $discoverImpactedSubjects->createJob($jobData);
    }

    public function testCreateJobSpecifyQueue()
    {
        $labeller = new \Tripod\Mongo\Labeller();

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias('http://example.com/1')=>array('rdf:type','spec:name'),
            $labeller->uri_to_alias('http://example.com/2')=>array('rdf:type','dct:title','dct:subject')
        );

        $jobData = array(
            'changes'=>$subjectsAndPredicatesOfChange,
            'operations'=>array(OP_SEARCH),
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
            'storeName'=>'tripod_php_testing',
            'podName'=>'CBD_testing',
            'contextAlias'=>'http://talisaspire.com/'
        );

        $queueName = \Tripod\Mongo\Config::getDiscoverQueueName() . '::TRIPOD_TESTING_QUEUE_' . uniqid();

        /** @var \Tripod\Mongo\Jobs\DiscoverImpactedSubjects|PHPUnit_Framework_MockObject_MockObject $discoverImpactedSubjects */
        $discoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('submitJob'))
            ->setMockClassName('MockDiscoverImpactedSubjects')
            ->getMock();

        $discoverImpactedSubjects->expects($this->once())
            ->method('submitJob')
            ->with(
                $queueName,
                'MockDiscoverImpactedSubjects',
                $jobData
            );

        $discoverImpactedSubjects->createJob($jobData, $queueName);
    }

    public function testManualQueueNamePersistsThroughJob()
    {
        /** @var \Tripod\Mongo\Jobs\DiscoverImpactedSubjects|PHPUnit_Framework_MockObject_MockObject $discoverImpactedSubjects */
        $discoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('getTripod', 'getApplyOperation'))
            ->getMock();

        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $views = $this->getMockBuilder('\Tripod\Mongo\Composites\Views')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(
                array(
                    'tripod_php_testing',
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/'
                )
            )->getMock();

        $tables = $this->getMockBuilder('\Tripod\Mongo\Composites\Tables')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(
                array(
                    'tripod_php_testing',
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/'
                )
            )->getMock();

        $search = $this->getMockBuilder('\Tripod\Mongo\Composites\SearchIndexer')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $this->setArgs();
        $args = $this->args;
        $args['queue'] = 'TRIPOD_TESTING_QUEUE_' . uniqid();
        $discoverImpactedSubjects->args = $args;

        $tripod->expects($this->exactly(3))
            ->method('getComposite')
            ->will($this->returnValueMap(
                array(
                    array(OP_VIEWS, $views),
                    array(OP_TABLES, $tables),
                    array(OP_SEARCH, $search)
                )
            ));

        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('createJob'))
            ->getMock();

        $viewSubject = new \Tripod\Mongo\ImpactedSubject(
            array(
                _ID_RESOURCE=>'http://example.com/resources/foo',
                _ID_CONTEXT=>$this->args['contextAlias']
            ),
            OP_VIEWS,
            $this->args['storeName'],
            $this->args['podName']
        );

        $views->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(
                array($viewSubject)
            ));

        $tableSubject = new \Tripod\Mongo\ImpactedSubject(
            array(
                _ID_RESOURCE=>'http://example.com/resources/foo2',
                _ID_CONTEXT=>$this->args['contextAlias']
            ),
            OP_TABLES,
            $this->args['storeName'],
            $this->args['podName'],
            array('t_foo_bar')
        );

        $tables->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(
                array($tableSubject)
            ));

        $search->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(array()));

        $discoverImpactedSubjects->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $discoverImpactedSubjects->expects($this->exactly(2))
            ->method('getApplyOperation')
            ->will($this->returnValue($applyOperation));

        $applyOperation->expects($this->exactly(2))
            ->method('createJob')
            ->withConsecutive(
                array(
                    array($viewSubject),
                    $args['queue']
                ),
                array(
                    array($tableSubject),
                    $args['queue']
                )
            );

        $discoverImpactedSubjects->perform();
    }

    public function testDiscoverOperationWillSubmitApplyOperationForDistinctQueues()
    {
        $config = \Tripod\Mongo\Config::getConfig();

        // Create a bunch of specs on various queues
        $tableSpecs = array(
            array(
                "_id"=>"t_resource",
                "type"=>"acorn:Resource",
                "from"=>"CBD_testing",
                "ensureIndexes" => array("value.isbn"=>1),
                "fields"=>array(
                    array("fieldName"=>"type","predicates"=>array("rdf:type")),
                    array("fieldName"=>"isbn","predicates"=>array("bibo:isbn13")),
                ),
                "joins"=>array(
                    "dct:isVersionOf"=>array(
                        "fields"=>array(
                            array("fieldName"=>"isbn13","predicates"=>array("bibo:isbn13"))
                        )
                    )
                )
            ),
            array(
                "_id"=>"t_source_count",
                "type"=>"acorn:Resource",
                "from"=>"CBD_testing",
                "to_data_source"=>"rs2",
                "queue"=>"counts_and_other_non_essentials",
                "fields"=>array(
                    array("fieldName"=>"type","predicates"=>array("rdf:type"))
                ),
                "joins"=>array(
                    "dct:isVersionOf"=>array(
                        "fields"=>array(
                            array("fieldName"=>"isbn13","predicates"=>array("bibo:isbn13"))
                        )
                    )
                ),
                "counts"=>array(
                    array("fieldName"=>"source_count", "property"=>"dct:isVersionOf"),
                    array("fieldName"=>"random_predicate_count", "property"=>"dct:randomPredicate")
                )
            ),
            array(
                "_id"=>"t_source_count_regex",
                "type"=>"acorn:Resource",
                "from"=>"CBD_testing",
                "queue"=>"counts_and_other_non_essentials",
                "fields"=>array(
                    array("fieldName"=>"type","predicates"=>array("rdf:type"))
                ),
                "joins"=>array(
                    "dct:isVersionOf"=>array(
                        "fields"=>array(
                            array("fieldName"=>"isbn13","predicates"=>array("bibo:isbn13"))
                        )
                    )
                ),
                "counts"=>array(
                    array("fieldName"=>"source_count", "property"=>"dct:isVersionOf"),
                    array("fieldName"=>"regex_source_count", "property"=>"dct:isVersionOf", "regex"=>"/foobar/")
                )
            ),
            array(
                "_id"=>"t_join_source_count_regex",
                "type"=>"acorn:Resource",
                "from"=>"CBD_testing",
                "queue"=>"MOST_IMPORTANT_QUEUE_EVER",
                "joins"=>array(
                    "acorn:jacsUri"=>array(
                        "counts"=>array(
                            array("fieldName"=>"titles_count","property"=>"dct:title")
                        )
                    )
                )
            )

        );

        $config['stores']['tripod_php_testing']['table_specifications'] = $tableSpecs;

        \Tripod\Mongo\Config::setConfig($config);

        /** @var \Tripod\Mongo\Jobs\DiscoverImpactedSubjects|PHPUnit_Framework_MockObject_MockObject $discoverImpactedSubjects */
        $discoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('getTripod', 'getApplyOperation'))
            ->getMock();

        $this->setArgs();
        $args = $this->args;
        $args['operations'] = array(OP_TABLES);
        $discoverImpactedSubjects->args = $args;

        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $tables = $this->getMockBuilder('\Tripod\Mongo\Composites\Tables')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(
                array(
                    'tripod_php_testing',
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/'
                )
            )->getMock();

        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('createJob'))
            ->getMock();

        $tableSubject1 = new \Tripod\Mongo\ImpactedSubject(
            array(
                _ID_RESOURCE=>'http://example.com/resources/foo2',
                _ID_CONTEXT=>$this->args['contextAlias']
            ),
            OP_TABLES,
            $this->args['storeName'],
            $this->args['podName'],
            array('t_resource','t_source_count','t_source_count_regex','t_join_source_count_regex')
        );

        $tableSubject2 = new \Tripod\Mongo\ImpactedSubject(
            array(
                _ID_RESOURCE=>'http://example.com/resources/foo3',
                _ID_CONTEXT=>$this->args['contextAlias']
            ),
            OP_TABLES,
            $this->args['storeName'],
            $this->args['podName'],
            array('t_resource','t_source_count')
        );


        $queuedTable1 = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>'http://example.com/resources/foo2',
                    _ID_CONTEXT=>$this->args['contextAlias']
                ),
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                array('t_resource')
            ),
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>'http://example.com/resources/foo3',
                    _ID_CONTEXT=>$this->args['contextAlias']
                ),
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                array('t_resource')
            ),
        );

        $queuedTable2 = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>'http://example.com/resources/foo2',
                    _ID_CONTEXT=>$this->args['contextAlias']
                ),
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                array('t_source_count','t_source_count_regex')
            ),
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>'http://example.com/resources/foo3',
                    _ID_CONTEXT=>$this->args['contextAlias']
                ),
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                array('t_source_count')
            ),
        );

        $queuedTable3 = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>'http://example.com/resources/foo2',
                    _ID_CONTEXT=>$this->args['contextAlias']
                ),
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                array('t_join_source_count_regex')
            )
        );

        $tables->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(array($tableSubject1, $tableSubject2)));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->will($this->returnValue($tables));

        $discoverImpactedSubjects->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $discoverImpactedSubjects->expects($this->exactly(3))
            ->method('getApplyOperation')
            ->will($this->returnValue($applyOperation));

        $applyOperation->expects($this->exactly(3))
            ->method('createJob')
            ->withConsecutive(
                array(
                    $queuedTable1,
                    \Tripod\Mongo\Config::getApplyQueueName()
                ),
                array(
                    $queuedTable2,
                    "counts_and_other_non_essentials"
                ),
                array(
                    $queuedTable3,
                    "MOST_IMPORTANT_QUEUE_EVER"
                )
            );

        $discoverImpactedSubjects->perform();
    }

    public function testManuallySpecifiedQueueWillOverrideQueuesDefinedInConfig()
    {
        $config = \Tripod\Mongo\Config::getConfig();

        // Create a bunch of specs on various queues
        $tableSpecs = array(
            array(
                "_id"=>"t_resource",
                "type"=>"acorn:Resource",
                "from"=>"CBD_testing",
                "ensureIndexes" => array("value.isbn"=>1),
                "fields"=>array(
                    array("fieldName"=>"type","predicates"=>array("rdf:type")),
                    array("fieldName"=>"isbn","predicates"=>array("bibo:isbn13")),
                ),
                "joins"=>array(
                    "dct:isVersionOf"=>array(
                        "fields"=>array(
                            array("fieldName"=>"isbn13","predicates"=>array("bibo:isbn13"))
                        )
                    )
                )
            ),
            array(
                "_id"=>"t_source_count",
                "type"=>"acorn:Resource",
                "from"=>"CBD_testing",
                "to_data_source"=>"rs2",
                "queue"=>"counts_and_other_non_essentials",
                "fields"=>array(
                    array("fieldName"=>"type","predicates"=>array("rdf:type"))
                ),
                "joins"=>array(
                    "dct:isVersionOf"=>array(
                        "fields"=>array(
                            array("fieldName"=>"isbn13","predicates"=>array("bibo:isbn13"))
                        )
                    )
                ),
                "counts"=>array(
                    array("fieldName"=>"source_count", "property"=>"dct:isVersionOf"),
                    array("fieldName"=>"random_predicate_count", "property"=>"dct:randomPredicate")
                )
            ),
            array(
                "_id"=>"t_source_count_regex",
                "type"=>"acorn:Resource",
                "from"=>"CBD_testing",
                "queue"=>"counts_and_other_non_essentials",
                "fields"=>array(
                    array("fieldName"=>"type","predicates"=>array("rdf:type"))
                ),
                "joins"=>array(
                    "dct:isVersionOf"=>array(
                        "fields"=>array(
                            array("fieldName"=>"isbn13","predicates"=>array("bibo:isbn13"))
                        )
                    )
                ),
                "counts"=>array(
                    array("fieldName"=>"source_count", "property"=>"dct:isVersionOf"),
                    array("fieldName"=>"regex_source_count", "property"=>"dct:isVersionOf", "regex"=>"/foobar/")
                )
            ),
            array(
                "_id"=>"t_join_source_count_regex",
                "type"=>"acorn:Resource",
                "from"=>"CBD_testing",
                "queue"=>"MOST_IMPORTANT_QUEUE_EVER",
                "joins"=>array(
                    "acorn:jacsUri"=>array(
                        "counts"=>array(
                            array("fieldName"=>"titles_count","property"=>"dct:title")
                        )
                    )
                )
            )

        );

        $config['stores']['tripod_php_testing']['table_specifications'] = $tableSpecs;

        \Tripod\Mongo\Config::setConfig($config);

        /** @var \Tripod\Mongo\Jobs\DiscoverImpactedSubjects|PHPUnit_Framework_MockObject_MockObject $discoverImpactedSubjects */
        $discoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('getTripod', 'getApplyOperation'))
            ->getMock();

        $this->setArgs();
        $args = $this->args;
        $args['operations'] = array(OP_TABLES);
        $args['queue'] = 'TRIPOD_TESTING_QUEUE_' . uniqid();
        $discoverImpactedSubjects->args = $args;

        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $tables = $this->getMockBuilder('\Tripod\Mongo\Composites\Tables')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(
                array(
                    'tripod_php_testing',
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/'
                )
            )->getMock();

        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('createJob'))
            ->getMock();

        $tableSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>'http://example.com/resources/foo2',
                    _ID_CONTEXT=>$this->args['contextAlias']
                ),
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                array('t_resource','t_source_count','t_source_count_regex','t_join_source_count_regex')
            ),
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>'http://example.com/resources/foo3',
                    _ID_CONTEXT=>$this->args['contextAlias']
                ),
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                array('t_distinct')
            )
        );

        $tables->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue($tableSubjects));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->will($this->returnValue($tables));

        $discoverImpactedSubjects->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $discoverImpactedSubjects->expects($this->once())
            ->method('getApplyOperation')
            ->will($this->returnValue($applyOperation));

        $applyOperation->expects($this->once())
            ->method('createJob')
            ->withConsecutive(
                array(
                    $tableSubjects,
                    $args['queue']
                )
            );


        $discoverImpactedSubjects->perform();
    }

    protected function setArgs()
    {
        $this->args = array(
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
            'storeName'=>'tripod_php_testing',
            'podName'=>'CBD_testing',
            'changes'=>array('http://example.com/resources/foo'=>array('rdf:type','dct:title')),
            'operations'=>array(OP_VIEWS, OP_TABLES, OP_SEARCH),
            'contextAlias'=>'http://talisaspire.com/'
        );
    }
}
