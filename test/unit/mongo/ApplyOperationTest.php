<?php

require_once 'MongoTripodTestBase.php';

/**
 * Class ApplyOperationTest
 */
class ApplyOperationTest extends MongoTripodTestBase
{
    protected $args = array();

    public function testMandatoryArgTripodConfig()
    {
        $this->setArgs();
        unset($this->args['tripodConfig']);
        $job = new \Tripod\Mongo\Jobs\ApplyOperation();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument tripodConfig was not present in supplied job args for job Tripod\Mongo\Jobs\ApplyOperation");
        $job->perform();
    }

    public function testMandatoryArgSubject()
    {
        $this->setArgs();
        unset($this->args['subjects']);
        $job = new \Tripod\Mongo\Jobs\ApplyOperation();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument subjects was not present in supplied job args for job Tripod\Mongo\Jobs\ApplyOperation");
        $job->perform();
    }

    public function testApplyViewOperation()
    {
        $this->setArgs();
        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('createImpactedSubject', 'getStat'))
            ->getMock();

        $applyOperation->args = $this->args;

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            array('timer','custom')
        );

        $subject = $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
            ->setMethods(array('getTripod'))
            ->setConstructorArgs(
                array(
                    array(
                        _ID_RESOURCE=>'http://example.com/resources/foo',
                        _ID_CONTEXT=>'http://talisaspire.com'
                    ),
                    OP_VIEWS,
                    'tripod_php_testing',
                    'CBD_testing'
                )
            )->getMock();

        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $views = $this->getMockBuilder('\Tripod\Mongo\Composites\Views')
            ->setMethods(array('update'))
            ->setConstructorArgs(array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                'http://talisapire.com/'
            ))->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $statMock->expects($this->once())
            ->method('custom')
            ->with(STAT_TYPE_COUNT, MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . STAT_TYPE_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                array(MONGO_QUEUE_APPLY_OPERATION.'.'.OP_VIEWS, $this->anything()),
                array(MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything())
            );

        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($views));

        $views->expects($this->once())
            ->method('update')
            ->with($subject);

        $applyOperation->perform();
    }

    public function testApplyTableOperation()
    {
        $this->setArgs();
        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('createImpactedSubject', 'getStat'))
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            array('timer','custom')
        );

        $impactedSubject = new \Tripod\Mongo\ImpactedSubject(
            array(
                _ID_RESOURCE=>'http://example.com/resources/foo',
                _ID_CONTEXT=>'http://talisaspire.com/'
            ),
            OP_TABLES,
            'tripod_php_testing',
            'CBD_testing',
            array('t_resource')
        );
        $this->args['subjects'] = array($impactedSubject->toArray());

        $applyOperation->args = $this->args;

        $subject = $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
            ->setMethods(array('getTripod'))
            ->setConstructorArgs(
                array(
                    array(
                        _ID_RESOURCE=>'http://example.com/resources/foo',
                        _ID_CONTEXT=>'http://talisaspire.com'
                    ),
                    OP_TABLES,
                    'tripod_php_testing',
                    'CBD_testing'
                )
            )->getMock();

        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $tables = $this->getMockBuilder('\Tripod\Mongo\Composites\Tables')
            ->setMethods(array('update'))
            ->setConstructorArgs(array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                'http://talisapire.com/'
            ))->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $statMock->expects($this->once())
            ->method('custom')
            ->with(STAT_TYPE_COUNT, MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . STAT_TYPE_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                array(MONGO_QUEUE_APPLY_OPERATION.'.'.OP_TABLES, $this->anything()),
                array(MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything())
            );


        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->will($this->returnValue($tables));

        $tables->expects($this->once())
            ->method('update')
            ->with($subject);

        $applyOperation->perform();
    }

    public function testApplySearchOperation()
    {
        $this->setArgs();
        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('createImpactedSubject', 'getStat'))
            ->getMock();

        $statMock = $this->getMockStat(
            $this->args['statsConfig']['config']['host'],
            $this->args['statsConfig']['config']['port'],
            $this->args['statsConfig']['config']['prefix'],
            array('timer','custom')
        );

        $impactedSubject = new \Tripod\Mongo\ImpactedSubject(
            array(
                _ID_RESOURCE=>'http://example.com/resources/foo',
                _ID_CONTEXT=>'http://talisaspire.com/'
            ),
            OP_SEARCH,
            'tripod_php_testing',
            'CBD_testing',
            array('t_resource')
        );
        $this->args['subjects'] = array($impactedSubject->toArray());

        $applyOperation->args = $this->args;

        $subject = $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
            ->setMethods(array('getTripod'))
            ->setConstructorArgs(
                array(
                    array(
                        _ID_RESOURCE=>'http://example.com/resources/foo',
                        _ID_CONTEXT=>'http://talisaspire.com'
                    ),
                    OP_SEARCH,
                    'tripod_php_testing',
                    'CBD_testing'
                )
            )->getMock();

        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $search = $this->getMockBuilder('\Tripod\Mongo\Composites\SearchIndexer')
            ->setMethods(array('update'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $applyOperation->expects($this->once())
            ->method('createImpactedSubject')
            ->will($this->returnValue($subject));

        $applyOperation->expects($this->exactly(3))
            ->method('getStat')
            ->will($this->returnValue($statMock));

        $statMock->expects($this->once())
            ->method('custom')
            ->with(STAT_TYPE_COUNT, MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . STAT_TYPE_COUNT, 1);
        $statMock->expects($this->exactly(2))
            ->method('timer')
            ->withConsecutive(
                array(MONGO_QUEUE_APPLY_OPERATION.'.'.OP_SEARCH, $this->anything()),
                array(MONGO_QUEUE_APPLY_OPERATION_SUCCESS, $this->anything())
            );


        $subject->expects($this->once())
            ->method('getTripod')
            ->will($this->returnValue($tripod));

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_SEARCH)
            ->will($this->returnValue($search));

        $search->expects($this->once())
            ->method('update')
            ->with($subject);

        $applyOperation->perform();
    }

    public function testCreateJobDefaultQueue()
    {
        $impactedSubject = new \Tripod\Mongo\ImpactedSubject(
            array(_ID_RESOURCE=>"http://example.com/1",_ID_CONTEXT=>"http://talisaspire.com/"),
            OP_TABLES,
            'tripod_php_testing',
            'CBD_testing',
            array('t_resource','t_resource_count')
        );

        $jobData = array(
            'subjects'=>array($impactedSubject->toArray()),
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
        );

        /** @var \Tripod\Mongo\Jobs\ApplyOperation|PHPUnit_Framework_MockObject_MockObject $applyOperation */
        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('submitJob'))
            ->setMockClassName('MockApplyOperation')
            ->getMock();

        $applyOperation->expects($this->once())
            ->method('submitJob')
            ->with(
                \Tripod\Mongo\Config::getApplyQueueName(),
                'MockApplyOperation',
                $jobData
            );

        $applyOperation->createJob(array($impactedSubject));
    }

    public function testCreateJobUnreachableRedis()
    {
        $impactedSubject = new \Tripod\Mongo\ImpactedSubject(
            array(_ID_RESOURCE=>"http://example.com/1",_ID_CONTEXT=>"http://talisaspire.com/"),
            OP_TABLES,
            'tripod_php_testing',
            'CBD_testing',
            array('t_resource','t_resource_count')
        );

        /** @var \Tripod\Mongo\Jobs\ApplyOperation|PHPUnit_Framework_MockObject_MockObject $applyOperation */
        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('warningLog','enqueue'))
            ->getMock();

        $e = new Exception("Connection to Redis failed after 1 failures.Last Error : (0) php_network_getaddresses: getaddrinfo failed: nodename nor servname provided, or not known");
        $applyOperation->expects($this->any())->method("enqueue")->will($this->throwException($e));

        // expect 5 retries. Catch this with call to warning log
        $applyOperation->expects($this->exactly(5))->method("warningLog");

        $exceptionThrown = false;
        try
        {
            $applyOperation->createJob(array($impactedSubject));
        }
        catch (\Tripod\Exceptions\JobException $e)
        {
            $this->assertEquals('Exception queuing job  - Connection to Redis failed after 1 failures.Last Error : (0) php_network_getaddresses: getaddrinfo failed: nodename nor servname provided, or not known',$e->getMessage());
            $exceptionThrown = true;
        }
        if (!$exceptionThrown) {
            $this->fail("Did not throw JobException");
        }
    }

    public function testCreateJobStatusFalse()
    {
        $impactedSubject = new \Tripod\Mongo\ImpactedSubject(
            array(_ID_RESOURCE=>"http://example.com/1",_ID_CONTEXT=>"http://talisaspire.com/"),
            OP_TABLES,
            'tripod_php_testing',
            'CBD_testing',
            array('t_resource','t_resource_count')
        );

        /** @var \Tripod\Mongo\Jobs\ApplyOperation|PHPUnit_Framework_MockObject_MockObject $applyOperation */
        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('enqueue','getJobStatus','warningLog'))
            ->getMock();

        $applyOperation->expects($this->any())->method("enqueue")->will($this->returnValue("sometoken"));
        $applyOperation->expects($this->any())->method("getJobStatus")->will($this->returnValue(false));

        // expect 5 retries. Catch this with call to warning log
        $applyOperation->expects($this->exactly(5))->method("warningLog");

        $exceptionThrown = false;
        try
        {
            $applyOperation->createJob(array($impactedSubject));
        }
        catch (\Tripod\Exceptions\JobException $e)
        {
            $this->assertEquals('Exception queuing job  - Could not retrieve status for queued job - job sometoken failed to tripod::apply',$e->getMessage());
            $exceptionThrown = true;
        }
        if (!$exceptionThrown) {
            $this->fail("Did not throw JobException");
        }
    }

    public function testCreateJobSpecifyQueue()
    {
        $impactedSubject = new \Tripod\Mongo\ImpactedSubject(
            array(_ID_RESOURCE=>"http://example.com/1",_ID_CONTEXT=>"http://talisaspire.com/"),
            OP_VIEWS,
            'tripod_php_testing',
            'CBD_testing',
            array()
        );

        $jobData = array(
            'subjects'=>array($impactedSubject->toArray()),
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
        );

        /** @var \Tripod\Mongo\Jobs\ApplyOperation|PHPUnit_Framework_MockObject_MockObject $applyOperation */
        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('submitJob'))
            ->setMockClassName('MockApplyOperation')
            ->getMock();

        $queueName = \Tripod\Mongo\Config::getApplyQueueName() . '::TRIPOD_TESTING_QUEUE_' . uniqid();

        $applyOperation->expects($this->once())
            ->method('submitJob')
            ->with(
                $queueName,
                'MockApplyOperation',
                $jobData
            );

        $applyOperation->createJob(array($impactedSubject), $queueName);
    }

    /**
     * Sets job arguments
     */
    protected function setArgs()
    {
        $subject = new \Tripod\Mongo\ImpactedSubject(
            array(
                _ID_RESOURCE=>'http://example.com/resources/foo',
                _ID_CONTEXT=>'http://talisaspire.com/'
            ),
            OP_VIEWS,
            'tripod_php_testing',
            'CBD_testing'
        );

        $this->args = array(
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
            'subjects'=>array($subject->toArray()),
            'statsConfig'=>$this->getStatsDConfig()
        );
    }
}