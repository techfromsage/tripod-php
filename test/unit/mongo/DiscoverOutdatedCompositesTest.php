<?php

require_once 'MongoTripodTestBase.php';
/**
 * Class DiscoverOutdatedCompositesTest
 */
class DiscoverOutdatedCompositesTest extends MongoTripodTestBase
{
    protected $args = array();
    
    public function testMandatoryArgTripodConfig() {
        $this->setArgs();
        unset($this->args['tripodConfig']);
        $job = new \Tripod\Mongo\Jobs\DiscoverOutdatedComposites();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument tripodConfig was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverOutdatedComposites");
        $job->perform();
    }

    public function testMandatoryArgStoreName() {
        $this->setArgs();
        unset($this->args['storeName']);
        $job = new \Tripod\Mongo\Jobs\DiscoverOutdatedComposites();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument storeName was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverOutdatedComposites");
        $job->perform();
    }

    public function testMandatoryArgCursorLimit() {
        $this->setArgs();
        unset($this->args['cursorLimit']);
        $job = new \Tripod\Mongo\Jobs\DiscoverOutdatedComposites();
        $job->args = $this->args;
        $this->setExpectedException('Exception', "Argument cursorLimit was not present in supplied job args for job Tripod\Mongo\Jobs\DiscoverOutdatedComposites");
        $job->perform();
    }

    public function testGetCompositeMetadataReturnsOnePerViewSpec() {
        $CONFIG = \Tripod\Mongo\Config::getInstance();
        $STORE_NAME = 'tripod_php_testing';
        $ALL_VIEW_SPECS = $CONFIG->getViewSpecifications('tripod_php_testing');

        $SPEC_KEY_REVISION = _SPEC_KEY.'.'._SPEC_REVISION;
        $SPEC_KEY_TYPE     = _SPEC_KEY.'.'._SPEC_TYPE;

        $this->setArgs();
        $discoverOutdatedComposites = new \Tripod\Mongo\Jobs\DiscoverOutdatedComposites();
        $discoverOutdatedComposites->args = $this->args;

        // fetch all composite metadata
        $compositeMetadata = $discoverOutdatedComposites->getCompositeMetadata($CONFIG, $STORE_NAME);

        // fetch the views information only
        $viewsOnly = array_filter($compositeMetadata, function($metadatum) { return $metadatum->compositeType === OP_VIEWS; });

        // there should be one metadata entry per view
        $this->assertCount(count($ALL_VIEW_SPECS), $viewsOnly);

        // each metadata entry should have view-specific information
        foreach ($viewsOnly as $viewMetadata) {
            $this->assertEquals(OP_VIEWS, $viewMetadata->compositeType, "type of composite is incorrect");
            $this->assertArrayHasKey(_ID_KEY, $viewMetadata->specification, "specification is not defined, or does not define an \"_id\" key");
            $this->assertEquals("views", $viewMetadata->compositeCollection->getName(), "does not reference a collection called \"views\"");
            $this->assertStringStartsWith("CBD_", $viewMetadata->cbdCollection->getName(), "does not reference a /^CBD_.*/ collection");

            // $queryComponent should be a $lt on the specified revision for the view
            $queryComponent = $viewMetadata->getOutdatedQueryComponent();

            $expectedQuery = array(
                $SPEC_KEY_TYPE     => $viewMetadata->specification['_id'],
                $SPEC_KEY_REVISION => array('$lt' => $viewMetadata->specification['_revision'])
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

    protected function setArgs() {
        $this->args = array(
            'tripodConfig' => \Tripod\Mongo\Config::getConfig(),
            'storeName'    => 'tripod_php_testing',
            'podName'      => 'CBD_testing',
            'cursorLimit'  => 50,
            'contextAlias' => 'http://talisaspire.com/'
        );
    }
}