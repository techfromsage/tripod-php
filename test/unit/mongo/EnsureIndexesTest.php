<?php

require_once 'MongoTripodTestBase.php';

/**
 * Class EnsureIndexes Test
 */
class EnsureIndexesTest extends MongoTripodTestBase
{
    /**
     * @var array
     */
    protected $args = array();

    protected function setUp()
    {
        $this->args = array(
            'tripodConfig' => '',
            'storeName'    => '',
            'reindex'      => ''
        );
        parent::setUp();
    }

    /**
     * Test exception is thrown if mandatory arguments are not set
     *
     * @dataProvider mandatoryArgDataProvider
     * @group ensure-indexes
     * @throws Exception
     */
    public function testMandatoryArgs($argumentName)
    {
        $job = new \Tripod\Mongo\Jobs\EnsureIndexes();
        $job->args = $this->args;
        $job->job->payload['id'] = uniqid();
        unset($job->args[$argumentName]);

        $this->setExpectedException('Exception', "Argument $argumentName was not present in supplied job args for job Tripod\Mongo\Jobs\EnsureIndexes");
        $job->perform();
    }

    /**
     * Data provider for testMandatoryArgs
     *
     * @return array
     */
    public function mandatoryArgDataProvider()
    {
        return array(
            array('tripodConfig'),
            array('storeName'),
            array('reindex')
        );
    }

    /**
     * Test the job behaves as expected
     */
    public function testSuccessfullyEnsureIndexesJob()
    {
        $job = $this->createJob();
        $job->args = $this->createDefaultArguments();
        $this->jobSuccessfullyEnsuresIndexes($job);

        $job->perform();
    }

    /**
     * Test that the job fails by throwing an exception
     */
    public function testEnsureIndexesJobThrowsErrorWhenCreatingIndexes()
    {
        $job = $this->createJob();
        $job->args = $this->createDefaultArguments();
        $this->jobThrowsExceptionWhenEnsuringIndexes($job);
        $this->setExpectedException('Exception', "Ensuring index failed");

        $job->perform();
    }
    /*
     *  HELPER METHODS BELOW HERE
     */

    /**
     *  Creates a simple mock EnsureIndexes Job
     *
     *  @return PHPUnit_Framework_MockObject_MockObject
     */
    protected function createJob()
    {
        $mockEnsureIndexesJob = $this->getMockBuilder('\Tripod\Mongo\Jobs\EnsureIndexes')
            ->setMethods(array('getIndexUtils'))
            ->getMock();

        $mockEnsureIndexesJob->job->payload['id'] = uniqid();
        return $mockEnsureIndexesJob;
    }

    /**
     * Returns default arguments for a EnsureIndexes Job
     * @return array
     */
    protected function createDefaultArguments()
    {
        $arguments = array(
            'tripodConfig' => \Tripod\Mongo\Config::getConfig(),
            'storeName'    => 'tripod_php_testing',
            'reindex'      => false
        );

        return $arguments;
    }

    /**
     * @param PHPUnit_Framework_MockObject_MockObject EnsureIndexes Job
     */
    protected function jobSuccessfullyEnsuresIndexes($job)
    {
        $mockIndexUtils = $this->getMockBuilder('\Tripod\Mongo\IndexUtils')
            ->setMethods(array('ensureIndexes'))
            ->getMock();

        $mockIndexUtils->expects($this->once())
            ->method('ensureIndexes')
            ->with(false, 'tripod_php_testing', true);

        $job->expects($this->once())
            ->method('getIndexUtils')
            ->will($this->returnValue($mockIndexUtils));
    }

    /**
     * @param PHPUnit_Framework_MockObject_MockObject EnsureIndexes Job
     */
    protected function jobThrowsExceptionWhenEnsuringIndexes($job)
    {
        $mockIndexUtils = $this->getMockBuilder('\Tripod\Mongo\IndexUtils')
            ->setMethods(array('ensureIndexes'))
            ->getMock();

        $mockIndexUtils->expects($this->once())
            ->method('ensureIndexes')
            ->with(false, 'tripod_php_testing', true)
            ->will($this->throwException(new \Exception("Ensuring index failed")));

        $job->expects($this->once())
            ->method('getIndexUtils')
            ->will($this->returnValue($mockIndexUtils));

    }
}
