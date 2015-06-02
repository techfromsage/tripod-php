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
        unset($this->args['subject']);
        $job = new \Tripod\Mongo\Jobs\ApplyOperation();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument subject was not present in supplied job args for job Tripod\Mongo\Jobs\ApplyOperation");
        $job->perform();
    }

    public function testApplyViewOperation()
    {
        $this->setArgs();
        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('createImpactedSubject'))
            ->getMock();

        $applyOperation->args = $this->args;

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
            ->setMethods(array('createImpactedSubject'))
            ->getMock();

        $this->setArgs();
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
        $this->args['subject'] = $impactedSubject->toArray();

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
            ->setMethods(array('createImpactedSubject'))
            ->getMock();

        $this->setArgs();
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
        $this->args['subject'] = $impactedSubject->toArray();

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
            'subject'=>$impactedSubject->toArray(),
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

        $applyOperation->createJob($impactedSubject);
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
            'subject'=>$impactedSubject->toArray(),
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
        );

        /** @var \Tripod\Mongo\Jobs\ApplyOperation|PHPUnit_Framework_MockObject_MockObject $applyOperation */
        $applyOperation = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(array('submitJob'))
            ->setMockClassName('MockApplyOperation')
            ->getMock();

        $queueName = 'TRIPOD_TESTING_QUEUE_' . uniqid();

        $applyOperation->expects($this->once())
            ->method('submitJob')
            ->with(
                $queueName,
                'MockApplyOperation',
                $jobData
            );

        $applyOperation->createJob($impactedSubject, $queueName);
    }

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
            'subject'=>$subject->toArray()
        );
    }
}