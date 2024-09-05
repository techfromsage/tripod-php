<?php

use Tripod\Mongo\Jobs\DiscoverImpactedSubjects;

class DiscoverImpactedSubjectsTest extends ResqueJobTestBase
{
    protected $args = [];

    public function testMandatoryArgTripodConfig()
    {
        $this->setArgs();
        unset($this->args['tripodConfig']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Argument tripodConfig or tripodConfigGenerator was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverImpactedSubjects');
        $this->performJob($job);
    }

    public function testMandatoryArgStoreName()
    {
        $this->setArgs();
        unset($this->args['storeName']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Argument storeName was not present in supplied job args for job Tripod\\Mongo\\Jobs\\DiscoverImpactedSubjects');
        $this->performJob($job);
    }

    public function testMandatoryArgPodName()
    {
        $this->setArgs();
        unset($this->args['podName']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Argument podName was not present in supplied job args for job Tripod\\Mongo\\Jobs\\DiscoverImpactedSubjects');
        $this->performJob($job);
    }

    public function testMandatoryArgChanges()
    {
        $this->setArgs();
        unset($this->args['changes']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Argument changes was not present in supplied job args for job Tripod\\Mongo\\Jobs\\DiscoverImpactedSubjects');
        $this->performJob($job);
    }

    public function testMandatoryArgOperations()
    {
        $this->setArgs();
        unset($this->args['operations']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Argument operations was not present in supplied job args for job Tripod\\Mongo\\Jobs\\DiscoverImpactedSubjects');
        $this->performJob($job);
    }

    public function testMandatoryArgContextAlias()
    {
        $this->setArgs();
        unset($this->args['contextAlias']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Argument contextAlias was not present in supplied job args for job Tripod\\Mongo\\Jobs\\DiscoverImpactedSubjects');
        $this->performJob($job);
    }

    public function testSubmitApplyOperationsJob()
    {
        $this->setArgs();

        $this->args['statsConfig'] = $this->getStatsDConfig();

        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $views = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getImpactedSubjects'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/',
                ]
            )->getMock();

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getImpactedSubjects'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/',
                ]
            )->getMock();

        $search = $this->getMockBuilder(Tripod\Mongo\Composites\SearchIndexer::class)
            ->onlyMethods(['getImpactedSubjects'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $tripod->expects($this->exactly(3))
            ->method('getComposite')
            ->will($this->returnValueMap(
                [
                    [OP_VIEWS, $views],
                    [OP_TABLES, $tables],
                    [OP_SEARCH, $search],
                ]
            ));

        $discoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['getTripod', 'getApplyOperation', 'getStat'])
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            ['timer', 'increment']
        );

        $discoverImpactedSubjects->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $discoverImpactedSubjects->args = $this->args;
        $discoverImpactedSubjects->job = new Resque_Job('queue', ['id' => uniqid()]);

        $applyOperation = $this->getMockBuilder(Tripod\Mongo\Jobs\ApplyOperation::class)
            ->onlyMethods(['createJob'])
            ->getMock();
        $applyOperation->job = new Resque_Job('queue', ['id' => uniqid()]);

        $viewSubject = new Tripod\Mongo\ImpactedSubject(
            [
                _ID_RESOURCE => 'http://example.com/resources/foo',
                _ID_CONTEXT => $this->args['contextAlias'],
            ],
            OP_VIEWS,
            $this->args['storeName'],
            $this->args['podName']
        );

        $views->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(
                [$viewSubject]
            ));

        $tableSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://example.com/resources/foo2',
                    _ID_CONTEXT => $this->args['contextAlias'],
                ],
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                ['t_foo_bar']
            ),
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://example.com/resources/foo3',
                    _ID_CONTEXT => $this->args['contextAlias'],
                ],
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                []
            ),
        ];

        $tables->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(
                $tableSubjects
            ));

        $search->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue([]));

        $discoverImpactedSubjects->expects($this->exactly(2))
            ->method('getApplyOperation')
            ->will($this->returnValue($applyOperation));

        $discoverImpactedSubjects->expects($this->exactly(5))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $applyOperation->expects($this->exactly(2))
            ->method('createJob')
            ->withConsecutive(
                [
                    [$viewSubject],
                    Tripod\Mongo\Config::getApplyQueueName(),
                    ['statsConfig' => $this->args['statsConfig']],
                ],
                [
                    $tableSubjects,
                    Tripod\Mongo\Config::getApplyQueueName(),
                    ['statsConfig' => $this->args['statsConfig']],
                ]
            );

        $statMock->expects($this->exactly(4))
            ->method('timer')
            ->withConsecutive(
                [MONGO_QUEUE_DISCOVER_SUBJECT, $this->anything()],
                [MONGO_QUEUE_DISCOVER_SUBJECT, $this->anything()],
                [MONGO_QUEUE_DISCOVER_SUBJECT, $this->anything()],
                [MONGO_QUEUE_DISCOVER_SUCCESS, $this->anything()]
            );

        $statMock->expects($this->once())
            ->method('increment')
            ->with(MONGO_QUEUE_DISCOVER_JOB . '.' . SUBJECT_COUNT, 3);

        $this->performJob($discoverImpactedSubjects);
    }

    public function testCreateJobDefaultQueue()
    {
        $labeller = new Tripod\Mongo\Labeller();

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias('http://example.com/1') => ['rdf:type', 'spec:name'],
            $labeller->uri_to_alias('http://example.com/2') => ['rdf:type', 'dct:title', 'dct:subject'],
        ];

        $jobData = [
            'changes' => $subjectsAndPredicatesOfChange,
            'operations' => [OP_SEARCH],
            'tripodConfig' => Tripod\Config::getConfig(),
            'storeName' => 'tripod_php_testing',
            'podName' => 'CBD_testing',
            'contextAlias' => 'http://talisaspire.com/',
        ];

        $discoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['submitJob'])
            ->setMockClassName('MockDiscoverImpactedSubjects')
            ->getMock();

        $discoverImpactedSubjects->expects($this->once())
            ->method('submitJob')
            ->with(
                Tripod\Mongo\Config::getDiscoverQueueName(),
                'MockDiscoverImpactedSubjects',
                $jobData
            );

        $discoverImpactedSubjects->createJob($jobData);
    }

    public function testCreateJobSpecifyQueue()
    {
        $labeller = new Tripod\Mongo\Labeller();

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias('http://example.com/1') => ['rdf:type', 'spec:name'],
            $labeller->uri_to_alias('http://example.com/2') => ['rdf:type', 'dct:title', 'dct:subject'],
        ];

        $jobData = [
            'changes' => $subjectsAndPredicatesOfChange,
            'operations' => [OP_SEARCH],
            'tripodConfig' => Tripod\Config::getConfig(),
            'storeName' => 'tripod_php_testing',
            'podName' => 'CBD_testing',
            'contextAlias' => 'http://talisaspire.com/',
        ];

        $queueName = Tripod\Mongo\Config::getDiscoverQueueName() . '::TRIPOD_TESTING_QUEUE_' . uniqid();

        $discoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['submitJob'])
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
        $discoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['getTripod', 'getApplyOperation'])
            ->getMock();

        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $views = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getImpactedSubjects'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/',
                ]
            )->getMock();

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getImpactedSubjects'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/',
                ]
            )->getMock();

        $search = $this->getMockBuilder(Tripod\Mongo\Composites\SearchIndexer::class)
            ->onlyMethods(['getImpactedSubjects'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $this->setArgs();
        $args = $this->args;
        $args['queue'] = 'TRIPOD_TESTING_QUEUE_' . uniqid();
        $discoverImpactedSubjects->args = $args;
        $discoverImpactedSubjects->job = new Resque_Job('queue', ['id' => uniqid()]);

        $tripod->expects($this->exactly(3))
            ->method('getComposite')
            ->will($this->returnValueMap(
                [
                    [OP_VIEWS, $views],
                    [OP_TABLES, $tables],
                    [OP_SEARCH, $search],
                ]
            ));

        $applyOperation = $this->getMockBuilder(Tripod\Mongo\Jobs\ApplyOperation::class)
            ->onlyMethods(['createJob'])
            ->getMock();

        $viewSubject = new Tripod\Mongo\ImpactedSubject(
            [
                _ID_RESOURCE => 'http://example.com/resources/foo',
                _ID_CONTEXT => $this->args['contextAlias'],
            ],
            OP_VIEWS,
            $this->args['storeName'],
            $this->args['podName']
        );

        $views->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(
                [$viewSubject]
            ));

        $tableSubject = new Tripod\Mongo\ImpactedSubject(
            [
                _ID_RESOURCE => 'http://example.com/resources/foo2',
                _ID_CONTEXT => $this->args['contextAlias'],
            ],
            OP_TABLES,
            $this->args['storeName'],
            $this->args['podName'],
            ['t_foo_bar']
        );

        $tables->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue(
                [$tableSubject]
            ));

        $search->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue([]));

        $discoverImpactedSubjects->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $discoverImpactedSubjects->expects($this->exactly(2))
            ->method('getApplyOperation')
            ->will($this->returnValue($applyOperation));

        $applyOperation->expects($this->exactly(2))
            ->method('createJob')
            ->withConsecutive(
                [
                    [$viewSubject],
                    $args['queue'],
                ],
                [
                    [$tableSubject],
                    $args['queue'],
                ]
            );

        $this->performJob($discoverImpactedSubjects);
    }

    public function testDiscoverOperationWillSubmitApplyOperationForDistinctQueues()
    {
        $config = Tripod\Config::getConfig();

        // Create a bunch of specs on various queues
        $tableSpecs = [
            [
                '_id' => 't_resource',
                'type' => 'acorn:Resource',
                'from' => 'CBD_testing',
                'ensureIndexes' => ['value.isbn' => 1],
                'fields' => [
                    ['fieldName' => 'type', 'predicates' => ['rdf:type']],
                    ['fieldName' => 'isbn', 'predicates' => ['bibo:isbn13']],
                ],
                'joins' => [
                    'dct:isVersionOf' => [
                        'fields' => [
                            ['fieldName' => 'isbn13', 'predicates' => ['bibo:isbn13']],
                        ],
                    ],
                ],
            ],
            [
                '_id' => 't_source_count',
                'type' => 'acorn:Resource',
                'from' => 'CBD_testing',
                'to_data_source' => 'rs2',
                'queue' => 'counts_and_other_non_essentials',
                'fields' => [
                    ['fieldName' => 'type', 'predicates' => ['rdf:type']],
                ],
                'joins' => [
                    'dct:isVersionOf' => [
                        'fields' => [
                            ['fieldName' => 'isbn13', 'predicates' => ['bibo:isbn13']],
                        ],
                    ],
                ],
                'counts' => [
                    ['fieldName' => 'source_count', 'property' => 'dct:isVersionOf'],
                    ['fieldName' => 'random_predicate_count', 'property' => 'dct:randomPredicate'],
                ],
            ],
            [
                '_id' => 't_source_count_regex',
                'type' => 'acorn:Resource',
                'from' => 'CBD_testing',
                'queue' => 'counts_and_other_non_essentials',
                'fields' => [
                    ['fieldName' => 'type', 'predicates' => ['rdf:type']],
                ],
                'joins' => [
                    'dct:isVersionOf' => [
                        'fields' => [
                            ['fieldName' => 'isbn13', 'predicates' => ['bibo:isbn13']],
                        ],
                    ],
                ],
                'counts' => [
                    ['fieldName' => 'source_count', 'property' => 'dct:isVersionOf'],
                    ['fieldName' => 'regex_source_count', 'property' => 'dct:isVersionOf', 'regex' => '/foobar/'],
                ],
            ],
            [
                '_id' => 't_join_source_count_regex',
                'type' => 'acorn:Resource',
                'from' => 'CBD_testing',
                'queue' => 'MOST_IMPORTANT_QUEUE_EVER',
                'joins' => [
                    'acorn:jacsUri' => [
                        'counts' => [
                            ['fieldName' => 'titles_count', 'property' => 'dct:title'],
                        ],
                    ],
                ],
            ],

        ];

        $config['stores']['tripod_php_testing']['table_specifications'] = $tableSpecs;

        Tripod\Config::setConfig($config);

        $discoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['getTripod', 'getApplyOperation'])
            ->getMock();

        $this->setArgs();
        $args = $this->args;
        $args['operations'] = [OP_TABLES];
        $discoverImpactedSubjects->args = $args;
        $discoverImpactedSubjects->job = new Resque_Job('queue', ['id' => uniqid()]);

        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getImpactedSubjects'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/',
                ]
            )->getMock();

        $applyOperation = $this->getMockBuilder(Tripod\Mongo\Jobs\ApplyOperation::class)
            ->onlyMethods(['createJob'])
            ->getMock();

        $tableSubject1 = new Tripod\Mongo\ImpactedSubject(
            [
                _ID_RESOURCE => 'http://example.com/resources/foo2',
                _ID_CONTEXT => $this->args['contextAlias'],
            ],
            OP_TABLES,
            $this->args['storeName'],
            $this->args['podName'],
            ['t_resource', 't_source_count', 't_source_count_regex', 't_join_source_count_regex']
        );

        $tableSubject2 = new Tripod\Mongo\ImpactedSubject(
            [
                _ID_RESOURCE => 'http://example.com/resources/foo3',
                _ID_CONTEXT => $this->args['contextAlias'],
            ],
            OP_TABLES,
            $this->args['storeName'],
            $this->args['podName'],
            ['t_resource', 't_source_count']
        );

        $queuedTable1 = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://example.com/resources/foo2',
                    _ID_CONTEXT => $this->args['contextAlias'],
                ],
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                ['t_resource']
            ),
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://example.com/resources/foo3',
                    _ID_CONTEXT => $this->args['contextAlias'],
                ],
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                ['t_resource']
            ),
        ];

        $queuedTable2 = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://example.com/resources/foo2',
                    _ID_CONTEXT => $this->args['contextAlias'],
                ],
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                ['t_source_count', 't_source_count_regex']
            ),
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://example.com/resources/foo3',
                    _ID_CONTEXT => $this->args['contextAlias'],
                ],
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                ['t_source_count']
            ),
        ];

        $queuedTable3 = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://example.com/resources/foo2',
                    _ID_CONTEXT => $this->args['contextAlias'],
                ],
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                ['t_join_source_count_regex']
            ),
        ];

        $tables->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($this->args['changes'], $this->args['contextAlias'])
            ->will($this->returnValue([$tableSubject1, $tableSubject2]));

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
                [
                    $queuedTable1,
                    Tripod\Mongo\Config::getApplyQueueName(),
                ],
                [
                    $queuedTable2,
                    'counts_and_other_non_essentials',
                ],
                [
                    $queuedTable3,
                    'MOST_IMPORTANT_QUEUE_EVER',
                ]
            );

        $this->performJob($discoverImpactedSubjects);
    }

    public function testManuallySpecifiedQueueWillOverrideQueuesDefinedInConfig()
    {
        $config = Tripod\Config::getConfig();

        // Create a bunch of specs on various queues
        $tableSpecs = [
            [
                '_id' => 't_resource',
                'type' => 'acorn:Resource',
                'from' => 'CBD_testing',
                'ensureIndexes' => ['value.isbn' => 1],
                'fields' => [
                    ['fieldName' => 'type', 'predicates' => ['rdf:type']],
                    ['fieldName' => 'isbn', 'predicates' => ['bibo:isbn13']],
                ],
                'joins' => [
                    'dct:isVersionOf' => [
                        'fields' => [
                            ['fieldName' => 'isbn13', 'predicates' => ['bibo:isbn13']],
                        ],
                    ],
                ],
            ],
            [
                '_id' => 't_source_count',
                'type' => 'acorn:Resource',
                'from' => 'CBD_testing',
                'to_data_source' => 'rs2',
                'queue' => 'counts_and_other_non_essentials',
                'fields' => [
                    ['fieldName' => 'type', 'predicates' => ['rdf:type']],
                ],
                'joins' => [
                    'dct:isVersionOf' => [
                        'fields' => [
                            ['fieldName' => 'isbn13', 'predicates' => ['bibo:isbn13']],
                        ],
                    ],
                ],
                'counts' => [
                    ['fieldName' => 'source_count', 'property' => 'dct:isVersionOf'],
                    ['fieldName' => 'random_predicate_count', 'property' => 'dct:randomPredicate'],
                ],
            ],
            [
                '_id' => 't_source_count_regex',
                'type' => 'acorn:Resource',
                'from' => 'CBD_testing',
                'queue' => 'counts_and_other_non_essentials',
                'fields' => [
                    ['fieldName' => 'type', 'predicates' => ['rdf:type']],
                ],
                'joins' => [
                    'dct:isVersionOf' => [
                        'fields' => [
                            ['fieldName' => 'isbn13', 'predicates' => ['bibo:isbn13']],
                        ],
                    ],
                ],
                'counts' => [
                    ['fieldName' => 'source_count', 'property' => 'dct:isVersionOf'],
                    ['fieldName' => 'regex_source_count', 'property' => 'dct:isVersionOf', 'regex' => '/foobar/'],
                ],
            ],
            [
                '_id' => 't_join_source_count_regex',
                'type' => 'acorn:Resource',
                'from' => 'CBD_testing',
                'queue' => 'MOST_IMPORTANT_QUEUE_EVER',
                'joins' => [
                    'acorn:jacsUri' => [
                        'counts' => [
                            ['fieldName' => 'titles_count', 'property' => 'dct:title'],
                        ],
                    ],
                ],
            ],

        ];

        $config['stores']['tripod_php_testing']['table_specifications'] = $tableSpecs;

        Tripod\Config::setConfig($config);

        $discoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['getTripod', 'getApplyOperation'])
            ->getMock();

        $this->setArgs();
        $args = $this->args;
        $args['operations'] = [OP_TABLES];
        $args['queue'] = 'TRIPOD_TESTING_QUEUE_' . uniqid();
        $discoverImpactedSubjects->args = $args;
        $discoverImpactedSubjects->job = new Resque_Job('queue', ['id' => uniqid()]);

        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getImpactedSubjects'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/',
                ]
            )->getMock();

        $applyOperation = $this->getMockBuilder(Tripod\Mongo\Jobs\ApplyOperation::class)
            ->onlyMethods(['createJob'])
            ->getMock();

        $tableSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://example.com/resources/foo2',
                    _ID_CONTEXT => $this->args['contextAlias'],
                ],
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                ['t_resource', 't_source_count', 't_source_count_regex', 't_join_source_count_regex']
            ),
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://example.com/resources/foo3',
                    _ID_CONTEXT => $this->args['contextAlias'],
                ],
                OP_TABLES,
                $this->args['storeName'],
                $this->args['podName'],
                ['t_distinct']
            ),
        ];

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
                [
                    $tableSubjects,
                    $args['queue'],
                ]
            );

        $this->performJob($discoverImpactedSubjects);
    }

    protected function setArgs()
    {
        $this->args = [
            'tripodConfig' => Tripod\Config::getConfig(),
            'storeName' => 'tripod_php_testing',
            'podName' => 'CBD_testing',
            'changes' => ['http://example.com/resources/foo' => ['rdf:type', 'dct:title']],
            'operations' => [OP_VIEWS, OP_TABLES, OP_SEARCH],
            'contextAlias' => 'http://talisaspire.com/',
        ];
    }
}
