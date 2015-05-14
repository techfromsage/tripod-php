<?php

require_once 'MongoTripodTestBase.php';

class ApplyOperationTest extends MongoTripodTestBase
{
    protected $args = array();

    public function testMandatoryArgTripodConfig()
    {
        $this->setArgs();
        unset($this->args['tripodConfig']);
        $job = new ApplyOperation();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument tripodConfig was not present in supplied job args for job ApplyOperation");
        $job->perform();
    }

    public function testMandatoryArgSubject()
    {
        $this->setArgs();
        unset($this->args['subject']);
        $job = new ApplyOperation();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument subject was not present in supplied job args for job ApplyOperation");
        $job->perform();
    }

    public function testApplyViewOperation()
    {
        $this->setArgs();
        $applyOperation = $this->getMockBuilder('ApplyOperation')
            ->setMethods(array('createImpactedSubject'))
            ->getMock();

        $applyOperation->args = $this->args;

        $subject = $this->getMockBuilder('ImpactedSubject')
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

        $tripod = $this->getMockBuilder('MongoTripod')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $views = $this->getMockBuilder('MongoTripodViews')
            ->setMethods(array('update'))
            ->setConstructorArgs(array(
                'tripod_php_testing',
                MongoTripodConfig::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
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
        $applyOperation = $this->getMockBuilder('ApplyOperation')
            ->setMethods(array('createImpactedSubject'))
            ->getMock();

        $this->setArgs();
        $impactedSubject = new ImpactedSubject(
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

        $subject = $this->getMockBuilder('ImpactedSubject')
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

        $tripod = $this->getMockBuilder('MongoTripod')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $tables = $this->getMockBuilder('MongoTripodTables')
            ->setMethods(array('update'))
            ->setConstructorArgs(array(
                'tripod_php_testing',
                MongoTripodConfig::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
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

    protected function setArgs()
    {
        $subject = new ImpactedSubject(
            array(
                _ID_RESOURCE=>'http://example.com/resources/foo',
                _ID_CONTEXT=>'http://talisaspire.com/'
            ),
            OP_VIEWS,
            'tripod_php_testing',
            'CBD_testing'
        );

        $this->args = array(
            'tripodConfig'=>MongoTripodConfig::getConfig(),
            'subject'=>$subject->toArray()
        );
    }
}