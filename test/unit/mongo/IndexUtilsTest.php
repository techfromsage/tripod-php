<?php

require_once 'MongoTripodTestBase.php';

/**
 * Class IndexUtilsTest
 */
class IndexUtilsTest extends MongoTripodTestBase
{
    public function testCBDCollectionIndexesAreCreated()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDIndexes($config);
        $this->deleteIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, true);
        $this->getCollectionForCBDShouldBeCalled_n_Times(4, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testCBDCollectionIndexesAreCreatedInForeground()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDIndexes($config);
        $this->deleteIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, false);
        $this->getCollectionForCBDShouldBeCalled_n_Times(4, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testCBDCollectionIndexesAreReindexed()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDIndexes($config);
        $this->deleteIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, true);
        $this->getCollectionForCBDShouldBeCalled_n_Times(5, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testViewIndexesAreCreated()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForViewIndexes($config);
        $this->deleteIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($collection, true);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($collection);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testViewIndexesAreCreatedInForeground()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForViewIndexes($config);
        $this->deleteIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($collection, false);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($collection);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testViewIndexesAreReindexed()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForViewIndexes($config);
        $this->deleteIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($collection, true);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($collection);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testTableIndexesAreCreated()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForTableIndexes($config);
        $this->deleteIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($collection, true);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testTableIndexesAreCreatedInForeground()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForTableIndexes($config);
        $this->deleteIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($collection, false);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testTableIndexesAreReindexed()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForTableIndexes($config);
        $this->deleteIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($collection, true);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testSearchDocIndexesAreCreated()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForSearchDocIndexes($config);
        $this->deleteIndexesShouldNeverBeCalled($collection);
        $this->threeInternalTripodSearchDocIndexesShouldBeCreated($collection, true);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testSearchDocIndexesAreCreatedInForeground()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForSearchDocIndexes($config);
        $this->deleteIndexesShouldNeverBeCalled($collection);
        $this->threeInternalTripodSearchDocIndexesShouldBeCreated($collection, false);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testSearchDocIndexesAreReindexed()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForSearchDocIndexes($config);
        $this->deleteIndexesShouldBeCalled($collection);
        $this->threeInternalTripodSearchDocIndexesShouldBeCreated($collection, true);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    /*
     * HELPER METHODS
     */

    /**
     * creates a mock IndexUtils object which will use the specified config
     *
     * @param PHPUnit_Framework_MockObject_MockObject mock config object
     * @return PHPUnit_Framework_MockObject_MockObject mocked IndexUtil object
     */
    protected function createMockIndexUtils($mockConfig)
    {
        $mockIndexUtils = $this->getMockBuilder('\Tripod\Mongo\IndexUtils')
            ->setMethods(array('getConfig'))
            ->getMock();

        $mockIndexUtils->expects($this->once())
            ->method('getConfig')
            ->will($this->returnValue($mockConfig));

        return $mockIndexUtils;
    }

    /**
     * creates a mock mongo collection object
     *
     * @param PHPUnit_Framework_MockObject_MockObject mock Collection object
     */
    protected function createMockCollection()
    {
        $mockCollection = $this->getMockBuilder('Collection')
            ->setMethods(array('createIndex', 'deleteIndexes'))
            ->getMock();

        return $mockCollection;
    }

    /**
     * creates a mock config object
     *
     * @param PHPUnit_Framework_MockObject_MockObject mock Config object
     */
    protected function createMockConfig()
    {
        $mockConfig = $this->getMockBuilder('TripodTestConfig')
            ->setMethods(array(
                'getCollectionForCBD',
                'getCollectionForView',
                'getCollectionForTable',
                'getCollectionForSearchDocument'
            ))
            ->getMock();

        return $mockConfig;
    }

    /**
     * @param int number of times Config->getCollectionForCBD should be called
     * @param PHPUnit_Framework_MockObject_MockObject mock Config object
     * @param PHPUnit_Framework_MockObject_MockObject mock Collection object
     */
    protected function getCollectionForCBDShouldBeCalled_n_Times($callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', 'CBD_testing')
            ->will($this->returnValue($mockCollection));
    }

    /**
     * @param int number of times Config->getCollectionForCBD should be called
     * @param PHPUnit_Framework_MockObject_MockObject mock Config object
     * @param PHPUnit_Framework_MockObject_MockObject mock Collection object
     */
    protected function getCollectionForViewShouldBeCalled_n_Times($callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForView')
            ->with('tripod_php_testing', 'v_testview')
            ->will($this->returnValue($mockCollection));
    }

    /**
     * @param int number of times Config->getCollectionForCBD should be called
     * @param PHPUnit_Framework_MockObject_MockObject mock Config object
     * @param PHPUnit_Framework_MockObject_MockObject mock Collection object
     */
    protected function getCollectionForTableShouldBeCalled_n_Times($callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForTable')
            ->with('tripod_php_testing', 't_testtable')
            ->will($this->returnValue($mockCollection));
    }

    /**
     * @param int number of times Config->getCollectionForCBD should be called
     * @param PHPUnit_Framework_MockObject_MockObject mock Config object
     * @param PHPUnit_Framework_MockObject_MockObject mock Collection object
     */
    protected function getCollectionForSearchDocShouldBeCalled_n_Times($callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForSearchDocument')
            ->with('tripod_php_testing', 'i_search_something')
            ->will($this->returnValue($mockCollection));
    }

    /**
     * @param PHPUnit_Framework_MockObject_MockObject mock Collection object
     */
    protected function deleteIndexesShouldNeverBeCalled($mockCollection)
    {
        $mockCollection->expects($this->never())
            ->method('deleteIndexes');
    }

    /**
     * @param PHPUnit_Framework_MockObject_MockObject mock Collection object
     */
    protected function deleteIndexesShouldBeCalled($mockCollection)
    {
        $mockCollection->expects($this->once())
            ->method('deleteIndexes');
    }

    /**
     * @param PHPUnit_Framework_MockObject_MockObject mock Config object
     */
    protected function getCollectionForViewShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForView');
    }

    /**
     * @param PHPUnit_Framework_MockObject_MockObject mock Config object
     */
    protected function getCollectionForTableShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForTable');
    }

    /**
     * @param PHPUnit_Framework_MockObject_MockObject mock Config object
     */
    protected function getCollectionForSearchDocumentShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForSearchDocument');
    }

    /**
     * @param PHPUnit_Framework_MockObject_MockObject mock Config object
     */
    protected function getCollectionForCBDShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForCBD');
    }

    /**
     * Expectations one custom and three internal tripod indexes should be
     * created
     *
     * createIndex is called 4 times, each time with a different set of params
     * a) one custom index is created based on the collection specification
     * b) three internal indexes are always created
     *
     * @param boolean create indexes in the background
     * @param PHPUnit_Framework_MockObject_MockObject mock Mongo Collection
     */
    protected function oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($mockCollection, $background=true)
    {
        // create index is called 4 times, each time with a different set of
        // params that we know.
        // a) one custom index is created based on the collection specification
        // b) three internal indexes are always created
        $mockCollection->expects($this->exactly(4))
            ->method('createIndex')
            ->withConsecutive(
                array(array('rdf:type.u' => 1), array('name'=>'rdf_type', 'background'=>$background)),
                array(array('_id' =>1 , '_lockedForTrans'=>1), array('name'=>'_lockedForTransIdx', 'background'=>$background)),
                array(array('_id' =>1 , '_uts'=>1), array('name'=>'_updatedTsIdx', 'background'=>$background)),
                array(array('_id' =>1 , '_cts'=>1), array('name'=>'_createdTsIdx', 'background'=>$background))
            );
    }

    /**
     * @param boolean create indexes in the background
     * @param PHPUnit_Framework_MockObject_MockObject mock Mongo Collection
     */
    protected function oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($mockCollection, $background=true)
    {
        // create index is called 4 times, each time with a different set of
        // params that we know.
        // a) one custom index is created based on the view specification
        // b) three internal indexes are always created
        $mockCollection->expects($this->exactly(4))
            ->method('createIndex')
            ->withConsecutive(
                array(array('_id.r' =>1, '_id.c'=>1, '_id.type'=>1), array('background'=>$background)),
                array(array('_id.type' => 1), array('background'=>$background)),
                array(array('value._impactIndex' =>1), array('background'=>$background)),
                array(array('rdf:type.u' => 1), array('background'=>$background))
            );
    }

    /**
     * @param boolean create indexes in the background
     * @param PHPUnit_Framework_MockObject_MockObject mock Mongo Collection
     */
    protected function oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($mockCollection, $background=true)
    {
        // create index is called 4 times, each time with a different set of
        // params that we know.
        // a) one custom index is created based on the view specification
        // b) three internal indexes are always created
        $mockCollection->expects($this->exactly(4))
            ->method('createIndex')
            ->withConsecutive(
                array(array('_id.r' =>1, '_id.c'=>1, '_id.type'=>1), array('background'=>$background)),
                array(array('_id.type' => 1), array('background'=>$background)),
                array(array('value._impactIndex' =>1), array('background'=>$background)),
                array(array('rdf:type.u' => 1), array('background'=>$background))
            );
    }

    /**
     * @param boolean create indexes in the background
     * @param PHPUnit_Framework_MockObject_MockObject mock Mongo Collection
     */
    protected function threeInternalTripodSearchDocIndexesShouldBeCreated($mockCollection, $background=true)
    {
        // create index is called 3 times, each time with a different set of
        // params that we know.
        // for search docs only internal indexes are created
        $mockCollection->expects($this->exactly(3))
            ->method('createIndex')
            ->withConsecutive(
                array(array('_id.r' =>1, '_id.c'=>1), array('background'=>$background)),
                array(array('_id.type' => 1), array('background'=>$background)),
                array(array('_impactIndex' =>1), array('background'=>$background))
            );

    }

    /**
     * Returns tripod config to use on with the IndexUtils object
     * This is a minimal config used to assert what should happen when ensuring
     * indexes for a CBD collection
     *
     * @param PHPUnit_Framework_MockObject_MockObject mock Tripod Config object
     */
    protected function setConfigForCBDIndexes($mockConfig)
    {

        // minimal config to verify that
        $config=array();
        $config["data_sources"] = array(
            "tlog"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://tloghost:27017,tloghost:27018/admin",
                "replicaSet" => "tlogrepset"
            ),
            "mongo"=>array("type"=>"mongo","connection" => "mongodb://localhost")
        );
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "type"=>"mongo",
                "data_source"=>"mongo",
                "pods" => array(
                    "CBD_testing" => array(
                        "indexes" => array(
                            "rdf_type" => array(
                                "rdf:type.u" => 1
                            )
                        )
                    )
                )
            )
        );

        $config["transaction_log"] = array(
            "database"=>"transactions",
            "collection"=>"transaction_log",
            "data_source"=>"tlog"
        );

        $mockConfig->loadConfig($config);
    }

    protected function setConfigForViewIndexes($mockConfig)
    {

        // minimal config to verify that
        $config=array();
        $config["data_sources"] = array(
            "tlog"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://tloghost:27017,tloghost:27018/admin",
                "replicaSet" => "tlogrepset"
            ),
            "mongo"=>array("type"=>"mongo","connection" => "mongodb://localhost")
        );
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "type"=>"mongo",
                "data_source"=>"mongo",
                "pods" => array("CBD_testing" => array()),
                "view_specifications" => array(
                    array(
                        "_id" => "v_testview",
                        "ensureIndexes" => array(
                            array("rdf:type.u"=>1)
                        ),
                        "from" => "CBD_testing",
                        "type" => "temp:TestType",
                        "joins" => array(
                            "dct:partOf" => array()
                        )
                    )
                )
            )
        );

        $config["transaction_log"] = array(
            "database"=>"transactions",
            "collection"=>"transaction_log",
            "data_source"=>"tlog"
        );

        $mockConfig->loadConfig($config);
    }

    protected function setConfigForTableIndexes($mockConfig)
    {

        // minimal config to verify that
        $config=array();
        $config["data_sources"] = array(
            "tlog"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://tloghost:27017,tloghost:27018/admin",
                "replicaSet" => "tlogrepset"
            ),
            "mongo"=>array("type"=>"mongo","connection" => "mongodb://localhost")
        );
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "type"=>"mongo",
                "data_source"=>"mongo",
                "pods" => array("CBD_testing" => array()),
                "table_specifications" => array(
                    array(
                        "_id" => "t_testtable",
                        "ensureIndexes" => array(
                            array("rdf:type.u"=>1)
                        ),
                        "from" => "CBD_testing",
                        "type" => "temp:TestType",
                        "fields" => array(
                            array(
                                "fieldName" => "fieldA",
                                "predicates" => array("spec:note")
                            )
                        )
                    )
                )
            )
        );

        $config["transaction_log"] = array(
            "database"=>"transactions",
            "collection"=>"transaction_log",
            "data_source"=>"tlog"
        );

        $mockConfig->loadConfig($config);
    }

    protected function setConfigForSearchDocIndexes($mockConfig)
    {

        // minimal config to verify that
        $config=array();
        $config["data_sources"] = array(
            "tlog"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://tloghost:27017,tloghost:27018/admin",
                "replicaSet" => "tlogrepset"
            ),
            "mongo"=>array("type"=>"mongo","connection" => "mongodb://localhost")
        );
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "type"=>"mongo",
                "data_source"=>"mongo",
                "pods" => array("CBD_testing" => array()),
                "search_config" => array(
                    "search_provider" => "MongoSearchProvider",
                    "search_specifications" => array(
                        array(
                            "_id" => "i_search_something",
                            "type" => "temp:TestType",
                            "from" => "CBD_testing",
                            "filter" => array(
                                "from" => "CBD_testing",
                                "condition" => array(
                                    "spec:name" => array(
                                        '$exists' => true
                                    )
                                )
                            ),
                            "fields" => array(
                                array(
                                    "fieldName" => "result.title",
                                    "predicates" => array("spec:note"),
                                    "limit" => 1
                                )
                            )
                        )
                    )
                )
            )
        );

        $config["transaction_log"] = array(
            "database"=>"transactions",
            "collection"=>"transaction_log",
            "data_source"=>"tlog"
        );

        $mockConfig->loadConfig($config);
    }
}
