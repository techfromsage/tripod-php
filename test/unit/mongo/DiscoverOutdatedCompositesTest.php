<?php

require_once 'MongoTripodTestBase.php';
/**
 * Class DiscoverOutdatedCompositesTest
 */
class DiscoverOutdatedCompositesTest extends MongoTripodTestBase
{
    const STORE_NAME = 'tripod_php_testing';

    // arguments passed to jobs
    protected $args = array();

    /**
     * @var \Tripod\Mongo\Composites\Views
     */
    protected $tripodViews = null;

    protected function setUp()
    {
        parent::setup();

        $this->tripodTransactionLog = new \Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        // Stub ouf 'addToElastic' search to prevent writes into Elastic Search happening by default.
        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $this->tripod */
        $this->tripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array('addToSearchIndexQueue'),
            array('CBD_testing','tripod_php_testing',
                array(
                    "defaultContext"=>"http://talisaspire.com/",
                    "async"=>array(OP_VIEWS=>false)
                )
            ) // don't generate views syncronously when saving automatically - let unit tests deal with this
        );
        $this->tripod->expects($this->any())->method('addToSearchIndexQueue');

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);

        $this->tripodViews = new \Tripod\Mongo\Composites\Views(
            $this->tripod->getStoreName(),
            $this->getTripodCollection($this->tripod),
            'http://talisaspire.com/'
        );

        foreach(\Tripod\Mongo\Config::getInstance()->getCollectionsForViews($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop();
        }

        // load base data
        $this->loadResourceDataViaTripod();
    }
    
    public function testMandatoryArgTripodConfig() {
        $job = $this->newDiscoverOutdatedComposites();
        unset($job->args['tripodConfig']);

        $this->setExpectedException('Exception', "Argument tripodConfig was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverOutdatedComposites");
        $job->perform();
    }

    public function testMandatoryArgStoreName() {
        $job = $this->newDiscoverOutdatedComposites();
        unset($job->args['storeName']);

        $this->setExpectedException('Exception', "Argument storeName was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverOutdatedComposites");
        $job->perform();
    }

    public function testMandatoryArgCursorLimit() {
        $job = $this->newDiscoverOutdatedComposites();
        unset($job->args['cursorLimit']);

        $this->setExpectedException('Exception', "Argument cursorLimit was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverOutdatedComposites");
        $job->perform();
    }

    public function testGetCompositeMetadataReturnsOnePerViewSpec() {
        $CONFIG = \Tripod\Mongo\Config::getInstance();
        $ALL_VIEW_SPECS = $CONFIG->getViewSpecifications(self::STORE_NAME);

        $SPEC_KEY_REVISION = _SPEC_KEY.'.'._SPEC_REVISION;
        $SPEC_KEY_TYPE     = _SPEC_KEY.'.'._SPEC_TYPE;

        $discoverOutdatedComposites = $this->newDiscoverOutdatedComposites();

        // fetch all composite metadata
        $compositeMetadata = $discoverOutdatedComposites->getCompositeMetadata($CONFIG, self::STORE_NAME);

        // fetch the views information only
        $viewsOnly = array_filter($compositeMetadata, function($metadatum) { return $metadatum->compositeType === COMPOSITE_TYPE_VIEWS; });

        // there should be one metadata entry per view
        $this->assertCount(count($ALL_VIEW_SPECS), $viewsOnly);

        // each metadata entry should have view-specific information
        foreach ($viewsOnly as $viewMetadata) {
            $this->assertEquals(COMPOSITE_TYPE_VIEWS, $viewMetadata->compositeType, "type of composite is incorrect");
            $this->assertArrayHasKey(_ID_KEY, $viewMetadata->specification, "specification is not defined, or does not define an \"_id\" key");
            $this->assertEquals("views", $viewMetadata->compositeCollection->getName(), "does not reference a collection called \"views\"");
            $this->assertStringStartsWith("CBD_", $viewMetadata->cbdCollection->getName(), "does not reference a /^CBD_.*/ collection");

            // $queryComponent should be a $lt on the specified revision for the view
            $queryComponent = $viewMetadata->getOutdatedQueryComponent();

            $expectedQuery = array(
                $SPEC_KEY_TYPE     => $viewMetadata->specification[_ID_KEY],
                $SPEC_KEY_REVISION => array('$lt' => $viewMetadata->specification[_REVISION])
            );
            $this->assertEquals($expectedQuery, $queryComponent);
        }

        // there's one metadata entry per view spec
        $meta2spec = function($meta) { return $meta->specification; };
        $this->assertEquals($ALL_VIEW_SPECS, array_map($meta2spec, $viewsOnly));
    }

    public function testGetCompositeMetadataReturnsOnePerTableSpec() {
        $this->markTestIncomplete('TODO: testGetCompositeMetadataReturnsOnePerViewSpec, but for table_rows');
    }

    public function testGetCompositeMetadataReturnsOnePerSearchSpec() {
        $this->markTestIncomplete('TODO: testGetCompositeMetadataReturnsOnePerViewSpec, but for searches');   
    }

    // -- CompositeMetadata->getOutdatedQueryComponent

    public function testGetOutdatedQueryComponentReturnsNullWhenNoRevisionOnViewSpec() {
        $CONFIG = \Tripod\Mongo\Config::getInstance();
        $VIEW_ID = 'v_resource_full';
        $VIEW_SPEC = $CONFIG->getViewSpecification(self::STORE_NAME, $VIEW_ID);

        // define a fake view specification with no revision
        $FAKE_VIEW_SPEC = $VIEW_SPEC;
        unset($FAKE_VIEW_SPEC[_REVISION]);

        // construct a CompositeMetadata around the fake view spec
        $compositeMetadata = new \Tripod\Mongo\Jobs\CompositeMetadata(COMPOSITE_TYPE_VIEWS, $FAKE_VIEW_SPEC, null, null);

        // the outdated query-component should be null when revision is missing
        $this->assertNull($compositeMetadata->getOutdatedQueryComponent(), 'For a CompositeMetadata whose spec has no "revision", getOutdatedQueryComponent() should return NULL.');
    }

    public function testGetOutdatedQueryComponentReturnsAQueryForValidRevisionOnViewSpec() {
        $CONFIG = \Tripod\Mongo\Config::getInstance();
        $VIEW_ID = 'v_resource_full';
        $VIEW_SPEC = $CONFIG->getViewSpecification(self::STORE_NAME, $VIEW_ID);

        $SPEC_KEY_REVISION = _SPEC_KEY.'.'._SPEC_REVISION;
        $SPEC_KEY_TYPE     = _SPEC_KEY.'.'._SPEC_TYPE;

        // define a fake view specification with no revision
        $FAKE_VIEW_SPEC = $VIEW_SPEC;
        $FAKE_VIEW_SPEC[_REVISION] = 1;

        // construct a CompositeMetadata around the fake view spec
        $compositeMetadata = 
            new \Tripod\Mongo\Jobs\CompositeMetadata(COMPOSITE_TYPE_VIEWS, $FAKE_VIEW_SPEC, null, null);

        // $queryComponent should be a $lt on the specified revision for the view
        $queryComponent = $compositeMetadata->getOutdatedQueryComponent();

        $expectedQuery = array(
            $SPEC_KEY_TYPE     => $VIEW_ID,
            $SPEC_KEY_REVISION => array('$lt' => $FAKE_VIEW_SPEC[_REVISION])
        );
        $this->assertEquals($expectedQuery, $queryComponent);
    }

    // -- DiscoverOutdatedComposites->getRegenTaskForMetadata

    public function testGetRegenTasksForMetadataReturnsNullWhenNoRevisionOnViewSpec() {
        $CONFIG = \Tripod\Mongo\Config::getInstance();
        $VIEW_ID = 'v_resource_full';
        $VIEW_SPEC = $CONFIG->getViewSpecification(self::STORE_NAME, $VIEW_ID);

        // mock a CompositeMetadata with a null outdated component
        $mockCompositeMetadata = 
            $this->getMockBuilder('\Tripod\Mongo\Jobs\CompositeMetadata')
                ->setMethods(array('getOutdatedQueryComponent'))
                ->setConstructorArgs(array(COMPOSITE_TYPE_VIEWS, $VIEW_SPEC, null, null))
                ->getMock();
        $mockCompositeMetadata->expects($this->once())
            ->method('getOutdatedQueryComponent')
            ->will($this->returnValue(null));

        
        // setup to call getRegenTaskForMetadata
        $discoverOutdatedComposites = $this->newDiscoverOutdatedComposites();

        $regenTask = $discoverOutdatedComposites
            ->getRegenTaskForMetadata($mockCompositeMetadata, 10);

        // ... which should return null
        $this->assertNull($regenTask, 'getRegenTaskForMetadata() should return NULL for a CompositeMetadata with NULL getOutdatedQueryComponent()');
    }


    public function testGetRegenTasksForMetadataReturnsNullWhenAllViewDocumentsUpToDate() {
        $CONFIG = \Tripod\Mongo\Config::getInstance();
        $VIEW_ID = 'v_resource_full';
        $VIEW_SPEC = $CONFIG->getViewSpecification(self::STORE_NAME, $VIEW_ID);

        $viewCollection = $CONFIG->getCollectionForView(self::STORE_NAME, $VIEW_SPEC[_ID_KEY]);
        $cbdCollection = $CONFIG->getFromCollectionForSpec(self::STORE_NAME, $VIEW_SPEC);

        // construct a CompositeMetadata around specified view spec
        $compositeMetadata = 
            new \Tripod\Mongo\Jobs\CompositeMetadata(
                COMPOSITE_TYPE_VIEWS, $VIEW_SPEC, $viewCollection, $cbdCollection
            );

        // setup to call getRegenTaskForMetadata
        $discoverOutdatedComposites = $this->newDiscoverOutdatedComposites();

        $regenTask = $discoverOutdatedComposites
            ->getRegenTaskForMetadata($compositeMetadata, 10);

        // ... which should return null
        $this->assertNull($regenTask, 'getRegenTaskForMetadata() should return NULL when there are no outdated documents in composite');
    }


    public function testGetRegenTasksForMetadataReturnsDocumentsToUpdateWhenViewRevised() {
        $CONFIG = \Tripod\Mongo\Config::getInstance();
        $VIEW_ID = 'v_resource_full';
        $VIEW_SPEC = $CONFIG->getViewSpecification(self::STORE_NAME, $VIEW_ID);

        $mongo = new MongoClient($CONFIG->getConnStr(self::STORE_NAME));
        $viewCollection = $CONFIG->getCollectionForView(self::STORE_NAME, $VIEW_SPEC[_ID_KEY]);

        // trigger generation for some particular views (so the view documents exist)
        $this->tripodViews->getViewForResource(
            "http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA",
            $VIEW_ID
        );

        // Create a fake view with an incremented revision
        // Now, all documents for this view will be treated as "outdated"
        $FAKE_VIEW_SPEC = $VIEW_SPEC;
        $FAKE_VIEW_SPEC[_REVISION] += 1;

        // fetch all documents from the view, and their CBD IDs. We will use these later
        // as expected values, since all of these should be marked "outdated"
        $filterToChosenView = array(_ID_KEY.'.'._ID_TYPE => $VIEW_ID);
        $fieldsIdOnly = array(_ID_KEY.'.'._ID_RESOURCE => 1);
        $allDocIdsFromView = 
            $mongo
                ->selectCollection(self::STORE_NAME, VIEWS_COLLECTION)
                ->find($filterToChosenView, $fieldsIdOnly);

        // extract the CBD resource IDs from the target views, for later comparison
        $doc2resource = function($doc) { return $doc[_ID_KEY][_ID_RESOURCE]; };
        $expectedCbdResourceIds = 
            array_map($doc2resource, iterator_to_array($allDocIdsFromView, false));

        // construct a CompositeMetadata around our fake view spec
        $cbdCollection = $CONFIG->getFromCollectionForSpec(self::STORE_NAME, $VIEW_SPEC);
        $compositeMetadata = 
            new \Tripod\Mongo\Jobs\CompositeMetadata(
                COMPOSITE_TYPE_VIEWS, $FAKE_VIEW_SPEC, $viewCollection, $cbdCollection
            );

        // setup to call getRegenTaskForMetadata
        $discoverOutdatedComposites = $this->newDiscoverOutdatedComposites();

        // UNDER TEST: this returns a task describing how to regenerate outdated CBDs
        $actualRegenTask = 
            $discoverOutdatedComposites->getRegenTaskForMetadata($compositeMetadata, 10);

        // the task should reference the right spec and composite collection
        $this->assertEquals($FAKE_VIEW_SPEC, $actualRegenTask->specification);
        $this->assertEquals($viewCollection, $actualRegenTask->compositeCollection);
        
        // the task should contain all root CBDs used to regenerate the chosen composites
        // However, it's hard to compare this directly, so extract CBD IDs and compare with
        // those extracted directly from the affected views
        $cbdDocs = iterator_to_array($actualRegenTask->cbdDocuments, false);
        $actualCbdResourceIds = array_map($doc2resource, $cbdDocs);

        $this->assertEquals($expectedCbdResourceIds, $actualCbdResourceIds);
    }

    // -- utility methods

    protected function newDiscoverOutdatedComposites() {
        $discoverOutdatedComposites = new \Tripod\Mongo\Jobs\DiscoverOutdatedComposites();
        $discoverOutdatedComposites->args = $this->getArgs();
        return $discoverOutdatedComposites;
    }

    protected function getArgs() {
        return array(
            'tripodConfig' => \Tripod\Mongo\Config::getConfig(),
            'storeName'    => 'tripod_php_testing',
            'podName'      => 'CBD_testing',
            'cursorLimit'  => 50,
            'contextAlias' => 'http://talisaspire.com/'
        );
    }
}