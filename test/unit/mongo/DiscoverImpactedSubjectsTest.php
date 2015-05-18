<?php

require_once 'MongoTripodTestBase.php';

use \Tripod\Mongo\DiscoverImpactedSubjects;
use \Tripod\Mongo\ImpactedSubject;

class DiscoverImpactedSubjectsTest extends MongoTripodTestBase
{
    protected $args = array();
    public function testMandatoryArgTripodConfig()
    {
        $this->setArgs();
        unset($this->args['tripodConfig']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument tripodConfig was not present in supplied job args for job DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgStoreName()
    {
        $this->setArgs();
        unset($this->args['storeName']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument storeName was not present in supplied job args for job DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgPodName()
    {
        $this->setArgs();
        unset($this->args['podName']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument podName was not present in supplied job args for job DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgChanges()
    {
        $this->setArgs();
        unset($this->args['changes']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument changes was not present in supplied job args for job DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgOperations()
    {
        $this->setArgs();
        unset($this->args['operations']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument operations was not present in supplied job args for job DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testMandatoryArgContextAlias()
    {
        $this->setArgs();
        unset($this->args['contextAlias']);
        $job = new DiscoverImpactedSubjects();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument contextAlias was not present in supplied job args for job DiscoverImpactedSubjects");
        $job->perform();
    }

    public function testSubmitApplyOperationsJob()
    {
        $this->setArgs();

        $tripod = $this->getMockBuilder('Tripod')
            ->setMethods(array('getComposite'))
            ->setConstructorArgs(array('CBD_testing', 'tripod_php_testing'))
            ->getMock();

        $views = $this->getMockBuilder('Views')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(
                array(
                    'tripod_php_testing',
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/'
                )
            )->getMock();

        $tables = $this->getMockBuilder('Tables')
            ->setMethods(array('getImpactedSubjects'))
            ->setConstructorArgs(
                array(
                    'tripod_php_testing',
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    'http://talisaspire.com/'
                )
            )->getMock();

        $search = $this->getMockBuilder('SearchIndexer')
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

        $discoverImpactedSubjects = $this->getMockBuilder('DiscoverImpactedSubjects')
            ->setMethods(array('getMongoTripod', 'submitJob'))
            ->getMock();

        $discoverImpactedSubjects->expects($this->once())
            ->method('getMongoTripod')
            ->will($this->returnValue($tripod));

        $discoverImpactedSubjects->args = $this->args;

        $viewSubject = new ImpactedSubject(
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

        $tableSubject = new ImpactedSubject(
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

        $discoverImpactedSubjects->expects($this->exactly(2))
            ->method('submitJob')
            ->withConsecutive(
                array(
                    \Tripod\Mongo\Config::getApplyQueueName(),
                    "ApplyOperation",
                    array(
                        "subject"=>$viewSubject->toArray(),
                        "tripodConfig"=>$this->args['tripodConfig']
                    )
                ),
                array(
                    \Tripod\Mongo\Config::getApplyQueueName(),
                    "ApplyOperation",
                    array(
                        "subject"=>$tableSubject->toArray(),
                        "tripodConfig"=>$this->args['tripodConfig']
                    )
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