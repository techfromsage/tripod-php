<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/ITripodStat.php';
require_once 'src/classes/StatsD.class.php';
require_once 'src/mongo/Driver.class.php';

/**
 * Class MongoTripodDriverTest
 */
class MongoTripodDriverTest extends MongoTripodTestBase
{
    /**
     * @var \Tripod\Mongo\Driver | PHPUnit_Framework_MockObject_MockObject
     */
    protected $tripod = null;
    /**
     * @var \Tripod\Mongo\TransactionLog
     */
    protected $tripodTransactionLog = null;

    protected function setUp()
    {
        parent::setup();

        $this->tripodTransactionLog = new \Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        // Stub ouf 'addToElastic' search to prevent writes into Elastic Search happening by default.
        $this->tripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array('addToSearchIndexQueue'),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array('defaultContext'=>'http://talisaspire.com/')
            )
        );

        $this->tripod->expects($this->any())->method('addToSearchIndexQueue');

        $this->getTripodCollection($this->tripod)->drop();

        // Lock collection no longer available from Tripod, so drop it manually
        \Tripod\Mongo\Config::getInstance()->getCollectionForLocks($this->tripod->getStoreName())->drop();

        $this->tripod->setTransactionLog($this->tripodTransactionLog);

        $this->loadBaseDataViaTripod();
    }

    public function testSelectMultiValue()
    {
        $expectedResult = array(
            "head" => array (
                "count"=>1,
                "offset"=>0,
                "limit"=>NULL
            ),
            "results"=> array(
                array(
                    "_id"=>array(
                        _ID_RESOURCE=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA",
                        _ID_CONTEXT=>"http://talisaspire.com/"),
                    "dct:source"=>array(
                        "http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53",
                        "http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9"
                    )
                )
            )
        );
        $actualResult = $this->tripod->select(array("bibo:isbn13.".VALUE_LITERAL=>"9780393929690"),array("dct:source"=>true));
        $this->assertEquals($expectedResult,$actualResult);
    }

    public function testSelectSingleValue()
    {
        $expectedResult = array(
            "head" => array (
                "count"=>1,
                "offset"=>0,
                "limit"=>NULL
            ),
            "results"=> array(
                array(
                    "_id"=>array(
                        _ID_RESOURCE=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA",
                        _ID_CONTEXT=>"http://talisaspire.com/"),
                    "dct:subject"=>"http://talisaspire.com/disciplines/physics"
                )
            )
        );
        $actualResult = $this->tripod->select(array("bibo:isbn13.".VALUE_LITERAL=>"9780393929690"),array("dct:subject"=>true));
        $this->assertEquals($expectedResult,$actualResult);
    }

    public function testDescribe()
    {
        $expectedResult =
            "<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/ontology/bibo/isbn13> \"9780393929690\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#bookmarkReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/bookmarks> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#foo> \"wibble\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f300> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f340> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#listReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/lists> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#openLibraryUri> <http://openlibrary.org/books/OL10157958M> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#preferredMetadata> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/metadata> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#author> \"Ohanian\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#discipline> \"physics\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#isbn> \"9780393929690\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Engineering: general\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"PHYSICS\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Science\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> \"Physics 3rd Edition\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> \"Physics for Engineers and Scientists\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"engineering: general\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"physics\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"science\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#usedAt> \"0071\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Resource> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/2002/07/owl#sameAs> <http://talisaspire.com/isbn/9780393929690> .
";
        $actualResult = $this->tripod->describe(array("bibo:isbn13.".VALUE_LITERAL=>"9780393929690"));
        $this->assertEquals($expectedResult,$actualResult->to_ntriples());
    }

    public function testDescribeResource()
    {
        $expectedResult =
            "<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/ontology/bibo/isbn13> \"9780393929690\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#bookmarkReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/bookmarks> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#foo> \"wibble\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f300> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f340> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#listReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/lists> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#openLibraryUri> <http://openlibrary.org/books/OL10157958M> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#preferredMetadata> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/metadata> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#author> \"Ohanian\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#discipline> \"physics\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#isbn> \"9780393929690\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Engineering: general\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"PHYSICS\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Science\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> \"Physics 3rd Edition\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> \"Physics for Engineers and Scientists\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"engineering: general\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"physics\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"science\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#usedAt> \"0071\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Resource> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/2002/07/owl#sameAs> <http://talisaspire.com/isbn/9780393929690> .
";
        $actualResult = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $this->assertEquals($expectedResult,$actualResult->to_ntriples());
    }

    public function testDescribeResources()
    {
        $expectedResult =
            "<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/ontology/bibo/isbn13> \"9780393929690\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#bookmarkReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/bookmarks> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#foo> \"wibble\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f300> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f340> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#listReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/lists> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#openLibraryUri> <http://openlibrary.org/books/OL10157958M> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#preferredMetadata> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/metadata> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#author> \"Ohanian\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#discipline> \"physics\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#isbn> \"9780393929690\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Engineering: general\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"PHYSICS\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Science\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> \"Physics 3rd Edition\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> \"Physics for Engineers and Scientists\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"engineering: general\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"physics\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"science\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#usedAt> \"0071\" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Resource> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/2002/07/owl#sameAs> <http://talisaspire.com/isbn/9780393929690> .
<http://talisaspire.com/works/4d101f63c10a6> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> .
<http://talisaspire.com/works/4d101f63c10a6> <http://purl.org/ontology/bibo/isbn13> \"9780393929691\" .
<http://talisaspire.com/works/4d101f63c10a6> <http://purl.org/ontology/bibo/isbn13> \"9780393929691-2\" .
<http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/searchTerms/schema#discipline> \"physics\" .
<http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/schema#seeAlso> <http://talisaspire.com/works/4d101f63c10a6-2> .
<http://talisaspire.com/works/4d101f63c10a6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> .
<http://talisaspire.com/works/4d101f63c10a6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Work> .
";
        $actualResult = $this->tripod->describeResources(array('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA','http://talisaspire.com/works/4d101f63c10a6'));
        $this->assertEquals($expectedResult,$actualResult->to_ntriples());
    }

    public function testGetCount()
    {
        $count = $this->tripod->getCount(array("rdf:type.".VALUE_URI=>"bibo:Book"));
        $this->assertEquals(5,$count);
    }

    public function testTripodSaveChangesRemovesLiteralTriple()
    {
        $oG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($oG);
        $nG->remove_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition');

        $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/",'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $this->assertEquals($nG, $uG, "Updated does not match expected graph");
    }

    public function testTripodSaveChangesAddsLiteralTriple()
    {
        $oG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($oG);
        $nG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/", 'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $this->assertEquals($nG, $uG,"Updated does not match expected graph");
    }

    /**
     * this test verifies that if we know we want to remove a specific triple from a document
     * we dont have to load the whole document in as the old graph, we just enumerate the single triple we want removed
     * what should happen is that the cs builder will translate that into a single removal
     */
    public function testTripodSaveChangesRemovesLiteralTripleUsingEmptyNewGraphAndPartialOldGraph()
    {
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $oG->qname_to_uri("bibo:isbn13"), "9780393929690");
        $nG = new \Tripod\Mongo\MongoGraph();

        $this->tripod->saveChanges($oG, $nG, "http://talisaspire.com/", 'my changes');

        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');

        $this->assertTrue($uG->has_triples_about('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA'));
        $this->assertDoesNotHaveLiteralTriple($uG,'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $oG->qname_to_uri("bibo:isbn13"), "9780393929690");
    }

    /**
     * this test verifies that if we simply want to add some data to a document that exists in we dont need to specify an oldgraph; we just need to specify the new graph
     * the cs builder should translate that into a single addition statement and apply it.
     */
    public function testTripodSaveChangesAddsLiteralTripleUsingEmptyOldGraph()
    {
        $oG = new \Tripod\Mongo\MongoGraph();
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($oG);
        $nG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/", 'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $this->assertHasLiteralTriple($uG, 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');
    }

    public function testTripodSaveChangesUpdatesLiteralTriple()
    {
        $oG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($oG);
        $nG->remove_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition');
        $nG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');
        //echo $nG->to_rdfxml()."\n";

        $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/", 'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        //echo $uG->to_rdfxml()."\n";

        $this->assertTrue($uG->has_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE'), "Graph should contain literal triple we added");
        $this->assertFalse($uG->has_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition'), "Graph should not contain literal triple we removed");
    }

    public function testSaveCompletelyNewGraph()
    {
        $uri = 'http://example.com/resources/1';

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g,"http://talisaspire.com/", "something new");

        $uG = $this->tripod->describeResource($uri);

        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
    }

    public function testRemoveGraphEntirely()
    {
        $uri = 'http://example.com/resources/1';

        // create a new entity and save it
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g,"http://talisaspire.com/", "something new");

        // retrieve it and make sure it was saved correctly
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");

        // now remove all knowledge about it, then describe the resource again, should be an empty graph
        $this->tripod->saveChanges($uG, new \Tripod\Mongo\MongoGraph(),"http://talisaspire.com/", "murder death kill");
        $g = $this->tripod->describeResource($uri);

        $this->assertTrue($g->is_empty());
    }

    public function testSaveFailsWhenOldGraphIsInvalidNoDataInStoreForObj()
    {
        $this->setExpectedException('Exception', 'Error storing changes');

        $uri = 'http://example.com/resources/1';

        // create a new entity and save it
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));

        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_resource_triple($uri, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:List"));

        $result = $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/");
    }

    public function testInterleavingUpdateFailsIfUnderlyingDataHasChanged()
    {
        $this->setExpectedException('Exception', 'Error storing changes');

        $uri = 'http://example.com/resources/1';

        $g = new \Tripod\Mongo\MongoGraph();
        // canned response will simulate that the underlying data has changed
        $doc = array("_id"=>$uri, "rdf:type"=>array(array('value'=>$g->qname_to_uri("acorn:Resource"), 'type'=>'uri')));

        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array('getDataUpdater'),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array('defaultContext'=>'http://talisaspire.com/')
            )
        );

        $mockTripodUpdate = $this->getMock(
            '\Tripod\Mongo\Updates',
            array('getDocumentForUpdate'),
            array($mockTripod)
        );

        $mockTripodUpdate->expects($this->once())
            ->method('getDocumentForUpdate')
            ->with($uri)
            ->will($this->returnValue($doc));

        $mockTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdate));
        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Book"));
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_resource_triple($uri, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:Foo"));

        $result = $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
    }

    public function testInterleavingUpdateFailsIfCriteriaIsNotValidAtPointOfSave()
    {
        $this->setExpectedException('Exception', 'Error storing changes');

        // save some data in the store
        $uri = 'http://example.com/resources/1';

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Book"));
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g,"http://talisaspire.com/");

        // canned response will simulate that the underlying data has changed
        $doc = array("_id"=>$uri, "_version"=>3,"rdf:type"=>array(array('value'=>$g->qname_to_uri("acorn:Resource"), 'type'=>'uri')));

        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array('getDataUpdater'),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array('defaultContext'=>'http://talisaspire.com/')
            )
        );

        $mockTripodUpdate = $this->getMock(
            '\Tripod\Mongo\Updates',
            array('getDocumentForUpdate'),
            array($mockTripod)
        );

        $mockTripodUpdate->expects($this->once())
            ->method('getDocumentForUpdate')
            ->with($uri)
            ->will($this->returnValue($doc));

        $mockTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdate));

        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));

        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_resource_triple($uri, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:Foo"));

        $result = $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
    }

    public function testAddMultipleTriplesForSameProperty()
    {
        $uri = 'http://example.com/resources/1';

        // save a graph to the store
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Some title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Another title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Yet another title");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $oG,"http://talisaspire.com/");

        // retrieve it and make sure it was saved correctly
        $g = $this->tripod->describeResource($uri);
        $this->assertTrue($g->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($g->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Some title"), "Graph should contain literal triple we added");
        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Another title"), "Graph should contain literal triple we added");
        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Yet another title"), "Graph should contain literal triple we added");
    }

    public function testRemoveMultipleTriplesForSameProperty()
    {
        $uri = 'http://example.com/resources/1';

        // save a graph to the store
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Some title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Another title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Yet another title");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $oG,"http://talisaspire.com/");

        // remove all three dct:title triples
        $g2 = new \Tripod\Mongo\MongoGraph();
        $g2->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $this->tripod->saveChanges($oG, $g2,"http://talisaspire.com/");

        $g = $this->tripod->describeResource($uri);
        $this->assertTrue($g->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($g->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Some title"), "Graph should not contain literal triple we removed");
        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Another title"), "Graph should not contain literal triple we removed");
        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Yet another title"), "Graph should not contain literal triple we removed");
    }

    public function testChangeMultipleTriplesForSamePropertySimple()
    {
        $uri = 'http://example.com/resources/1';

        // save a graph to the store
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Some title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Another title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Yet another title");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $oG,"http://talisaspire.com/");

        $g2= new \Tripod\Mongo\MongoGraph();
        $g2->add_resource_triple($uri, $g2->qname_to_uri("rdf:type"), $g2->qname_to_uri("acorn:Resource"));
        $g2->add_literal_triple($uri, $g2->qname_to_uri("dct:title"), "Updated Some title");
        $g2->add_literal_triple($uri, $g2->qname_to_uri("dct:title"), "Updated Another title");
        $g2->add_literal_triple($uri, $g2->qname_to_uri("dct:title"), "Updated Yet another title");
        $this->tripod->saveChanges($oG, $g2,"http://talisaspire.com/");

        $g = $this->tripod->describeResource($uri);

        $this->assertTrue($g->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($g->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");

        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Some title"), "Graph should not contain literal triple we removed");
        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Another title"), "Graph should not contain literal triple we removed");
        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Yet another title"), "Graph should not contain literal triple we removed");

        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Updated Some title"), "Graph should  contain literal triple we added");
        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Updated Another title"), "Graph should contain literal triple we added");
        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Updated Yet another title"), "Graph should contain literal triple we added");

    }

    public function testChangeMultipleTriplesForSamePropertyMoreComplex()
    {
        $uri = 'http://example.com/resources/1';

        // save a graph to the store
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title one");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title two");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title three");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title four");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title five");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $oG,"http://talisaspire.com/");

        // new data
        $g2= new \Tripod\Mongo\MongoGraph();
        $g2->add_resource_triple($uri, $g2->qname_to_uri("rdf:type"), $g2->qname_to_uri("acorn:Resource"));
        $g2->add_literal_triple($uri, $g2->qname_to_uri("dct:title"), "New Title one");
        $g2->add_literal_triple($uri, $g2->qname_to_uri("dct:title"), "New Title two");
        $g2->add_literal_triple($uri, $g2->qname_to_uri("dct:title"), "Title five");
        $g2->add_literal_triple($uri, $g2->qname_to_uri("dct:title"), "New Title seven");
        $this->tripod->saveChanges($oG, $g2,"http://talisaspire.com/");

        $g = $this->tripod->describeResource($uri);

        $this->assertTrue($g->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($g->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");

        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Title one"), "Graph should not contain literal triple we removed");
        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Title two"), "Graph should not contain literal triple we removed");
        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Title three"), "Graph should not contain literal triple we removed");
        $this->assertFalse($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Title four"), "Graph should not contain literal triple we removed");

        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "New Title one"), "Graph should  contain literal triple we added");
        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "New Title two"), "Graph should contain literal triple we added");
        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "Title five"), "Graph should contain literal triple we added");
        $this->assertTrue($g->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "New Title seven"), "Graph should contain literal triple we added");
    }

    public function testSetReadPreferenceWhenSavingChanges(){
        $subjectOne = "http://talisaspire.com/works/checkReadPreferencesWrite";
        /** @var $tripodMock \Tripod\Mongo\Driver **/
        $tripodMock = $this->getMock(
            '\Tripod\Mongo\Driver', 
            array('getDataUpdater'), 
            array(
                'CBD_testing',
                'tripod_php_testing',
                array('defaultContext'=>'http://talisaspire.com/')
            )
        );
        
        $tripodUpdate = $this->getMock(
            '\Tripod\Mongo\Updates',
            array('addToSearchIndexQueue','setReadPreferenceToPrimary','resetOriginalReadPreference'), 
            array($tripodMock)
        );
        
        $tripodUpdate
            ->expects($this->once(0))
            ->method('setReadPreferenceToPrimary');

        $tripodUpdate
            ->expects($this->once())
            ->method('resetOriginalReadPreference');
        
        $tripodMock
            ->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title"), "Title one");
        $tripodMock->saveChanges(new \Tripod\Mongo\MongoGraph(), $g,"http://talisaspire.com/");
    }

    /**
     * @expectedException Exception
     */
    public function testReadPreferencesAreRestoredWhenErrorSavingChanges(){
        $subjectOne = "http://talisaspire.com/works/checkReadPreferencesAreRestoredOnError";
        /** @var $tripodMock \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject **/
        $tripodMock = $this->getMock(
            '\Tripod\Mongo\Driver', 
            array('getDataUpdater'), 
            array(
                'CBD_testing',
                'tripod_php_testing',
                array('defaultContext'=>'http://talisaspire.com/')
            )
        );

        /** @var $tripodUpdate \Tripod\Mongo\Updates|PHPUnit_Framework_MockObject_MockObject **/
        $tripodUpdate = $this->getMock(
            '\Tripod\Mongo\Updates',
            array('addToSearchIndexQueue','resetOriginalReadPreference','getContextAlias'), 
            array($tripodMock)
        );

        $tripodUpdate
            ->expects($this->once())
            ->method('getContextAlias')
            ->will($this->throwException(new Exception("A Test Exception")));

        $tripodUpdate
            ->expects($this->once())
            ->method('resetOriginalReadPreference');

        $tripodMock
            ->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title"), "Title one");
        $tripodMock->saveChanges(new \Tripod\Mongo\MongoGraph(), $g,"http://talisaspire.com/");
    }

    public function testReadPreferencesOverMultipleSaves(){
        $subjectOne = "http://talisaspire.com/works/checkReadPreferencesOverMultipleSaves";
        /** @var TestTripod $tripodMock **/
        $tripodMock = $this->getMock(
            'TestTripod',
            array('getDataUpdater'),
            array('CBD_testing','tripod_php_testing',
                array('defaultContext'=>'http://talisaspire.com/', 'readPreference'=>MongoClient::RP_SECONDARY_PREFERRED))
        );

        $tripodUpdate = $this->getMock('\Tripod\Mongo\Updates',
            array('addToSearchIndexQueue', 'validateGraphCardinality'), array($tripodMock));
        $tripodUpdate
            ->expects($this->any())
            ->method('addToSearchIndexQueue');

        $tripodUpdate
            ->expects($this->exactly(3))
            ->method('validateGraphCardinality')
            ->will($this->onConsecutiveCalls(null, $this->throwException(new Exception('readPreferenceOverMultipleSavesTestException')), null)
            );

        $tripodMock
            ->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $expectedCollectionReadPreference = $tripodMock->getCollectionReadPreference();
        $this->assertEquals($expectedCollectionReadPreference['type'], MongoClient::RP_SECONDARY_PREFERRED);

        // Assert that a simple save results in read preferences being restored
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title"), "Title one");
        $tripodMock->saveChanges(new \Tripod\Mongo\MongoGraph(), $g,"http://talisaspire.com/");
        $this->assertEquals($expectedCollectionReadPreference, $tripodMock->getCollectionReadPreference());

        // Assert a thrown exception still results in read preferences being restored
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title2"), "Title two");
        $exceptionThrown = false;
        try{
            $tripodMock->saveChanges(new \Tripod\Mongo\MongoGraph(), $g,"http://talisaspire.com/");
        }
        catch(Exception $e){
            $exceptionThrown = true;
            $this->assertEquals("readPreferenceOverMultipleSavesTestException", $e->getMessage());
        }
        $this->assertTrue($exceptionThrown);
        $this->assertEquals($expectedCollectionReadPreference, $tripodMock->getCollectionReadPreference());

        // Assert that a new save after the exception still results in read preferences being restored
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title3"), "Title three");
        $tripodMock->saveChanges(new \Tripod\Mongo\MongoGraph(), $g,"http://talisaspire.com/");
        $this->assertEquals($expectedCollectionReadPreference, $tripodMock->getCollectionReadPreference());

    }

    public function testSaveChangesToLockedDocument()
    {
        $subjectOne = "http://talisaspire.com/works/lockedDoc";

        $this->lockDocument($subjectOne,"transaction_101");

        $this->setExpectedException('\Exception', "Error storing changes: Did not obtain locks on documents");

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title"), "Title one");

        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g,"http://talisaspire.com/");

    }

    public function testSaveChangesToMultipleSubjects()
    {
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';

        // save a graph to the store containng two completely new entities
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($subjectOne, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($subjectOne, $oG->qname_to_uri("dct:title"), "Title one");
        $oG->add_literal_triple($subjectOne, $oG->qname_to_uri("dct:title"), "Title two");
        $oG->add_resource_triple($subjectTwo, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Book"));
        $oG->add_literal_triple($subjectTwo, $oG->qname_to_uri("dct:title"), "Title three");
        $oG->add_literal_triple($subjectTwo, $oG->qname_to_uri("dct:title"), "Title four");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $oG,"http://talisaspire.com/");

        // retrieve them both, assert they are as we expect
        $g = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($g->has_triples_about($subjectOne));
        $this->assertTrue($g->has_resource_triple($subjectOne, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")));
        $this->assertTrue($g->has_literal_triple($subjectOne,  $g->qname_to_uri("dct:title"), 'Title one'));
        $this->assertTrue($g->has_literal_triple($subjectOne,  $g->qname_to_uri("dct:title"), 'Title two'));
        $this->assertTrue($g->has_triples_about($subjectTwo));
        $this->assertTrue($g->has_resource_triple($subjectTwo, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Book")));
        $this->assertTrue($g->has_literal_triple($subjectTwo,  $g->qname_to_uri("dct:title"), 'Title three'));
        $this->assertTrue($g->has_literal_triple($subjectTwo,  $g->qname_to_uri("dct:title"), 'Title four'));

        // now lets save some changes to both
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($g);
        $nG->remove_literal_triple($subjectOne,  $g->qname_to_uri("dct:title"), 'Title one');
        $nG->add_literal_triple($subjectOne,  $g->qname_to_uri("dct:title"), 'Updated Title one');
        $nG->add_literal_triple($subjectOne,  $g->qname_to_uri("dct:author"), 'Joe Bloggs');
        $nG->remove_literal_triple($subjectTwo,  $g->qname_to_uri("dct:title"), 'Title four');
        $nG->remove_resource_triple($subjectTwo,  $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Book"));
        $nG->add_literal_triple($subjectTwo,  $g->qname_to_uri("dct:title"), 'Updated Title four');
        $nG->add_literal_triple($subjectTwo,  $g->qname_to_uri("dct:author"), 'James Brown');

        $this->tripod->saveChanges($g, $nG,"http://talisaspire.com/");

        $uG = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($uG->has_triples_about($subjectOne));
        $this->assertTrue($uG->has_resource_triple($subjectOne, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Resource")));
        $this->assertTrue($uG->has_literal_triple($subjectOne,  $uG->qname_to_uri("dct:title"), 'Updated Title one'));
        $this->assertFalse($uG->has_literal_triple($subjectOne,  $uG->qname_to_uri("dct:title"), 'Title one'));
        $this->assertTrue($uG->has_literal_triple($subjectOne,  $uG->qname_to_uri("dct:title"), 'Title two'));
        $this->assertTrue($uG->has_literal_triple($subjectOne,  $uG->qname_to_uri("dct:author"), 'Joe Bloggs'));

        $this->assertTrue($uG->has_triples_about($subjectTwo));
        $this->assertFalse($uG->has_resource_triple($subjectTwo, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Book")));
        $this->assertTrue($uG->has_literal_triple($subjectTwo,  $uG->qname_to_uri("dct:title"), 'Title three'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo,  $uG->qname_to_uri("dct:title"), 'Updated Title four'));
        $this->assertFalse($uG->has_literal_triple($subjectTwo,  $uG->qname_to_uri("dct:title"), 'Title four'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo,  $uG->qname_to_uri("dct:author"), 'James Brown'));
    }

    public function testDocumentVersioning()
    {
        $uri = 'http://example.com/resources/1';

        // save a new entity, and retrieve it
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g, "http://talisaspire.com/", "something new");
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 0);

        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($g);
        $nG->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "another title");
        $this->tripod->saveChanges($g, $nG,"http://talisaspire.com/");
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "another title"), "Graph should contain literal triple we added");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 1);

        $nG = new \Tripod\Mongo\MongoGraph();
        //$nG->add_graph();
        $nG->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "only a title");
        $this->tripod->saveChanges($uG, $nG,"http://talisaspire.com/");

        $uG = $this->tripod->describeResource($uri);
        $this->assertFalse($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should not contain resource triple we removed");
        $this->assertFalse($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should not contain literal triple we removed");
        $this->assertFalse($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "another title"), "Graph should not contain literal triple we removed");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "only a title"), "Graph should contain literal triple we added");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 2);

        // remove it completely
        $this->tripod->saveChanges($nG, new \Tripod\Mongo\MongoGraph(),"http://talisaspire.com/");
        $this->assertDocumentHasBeenDeleted(array("r"=>$uri,"c"=>"http://talisaspire.com/"));
    }

    public function testSaveChangesWithInvalidCardinality()
    {
        $this->setExpectedException('\Tripod\Exceptions\CardinalityException', "Cardinality failed on http://foo/bar/1 for 'rdf:type' - should only have 1 value and has: http://foo/bar#Class1, http://foo/bar#Class2");

        $config = array();
        $config['namespaces'] = array('rdf'=>'http://www.w3.org/1999/02/22-rdf-syntax-ns#');
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"tlog");
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://localhost"
            ),
            "tlog"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://abc:xyz@localhost:27018"
            )
        );
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array(
                        "cardinality"=>array(
                            "rdf:type"=>1
                        )
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"queue","collection"=>"q_queue","data_source"=>"db");

        // Override the config defined in base test class as we need specific config here.
        \Tripod\Mongo\Config::setConfig($config);

        $tripod = new \Tripod\Mongo\Driver('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/'));

        $oldGraph = new \Tripod\ExtendedGraph();
        $newGraph = new \Tripod\ExtendedGraph();
        $newGraph->add_resource_triple('http://foo/bar/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://foo/bar#Class1');
        $newGraph->add_resource_triple('http://foo/bar/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://foo/bar#Class2');

        $tripod->saveChanges($oldGraph, $newGraph,"http://talisaspire.com/");
    }


    public function testDiscoverImpactedSubjectsAreDoneAllOperationsASync()
    {
        $uri_1 = "http://example.com/1";
        $uri_2 = "http://example.com/2";
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri_1, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_resource_triple($uri_2, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));

        // just updates, all three operations async
        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array(
                'getDataUpdater',
                'getComposite'
            ),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>true,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockTripodUpdates = $this->getMock(
            '\Tripod\Mongo\Updates',
            array(
                'storeChanges',
                'getDiscoverImpactedSubjects'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>true,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockDiscoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('createJob'))
            ->getMock();

        $labeller = new \Tripod\Mongo\Labeller();

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias('http://example.com/1')=>array('rdf:type'),
            $labeller->uri_to_alias('http://example.com/2')=>array('rdf:type')
        );

        $jobData = array(
            'changes'=>$subjectsAndPredicatesOfChange,
            'operations'=>array(OP_TABLES, OP_VIEWS, OP_SEARCH),
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
            'storeName'=>'tripod_php_testing',
            'podName'=>'CBD_testing',
            'contextAlias'=>'http://talisaspire.com/'
        );

        // getComposite() should only be called if there are synchronous operations
        $mockTripod->expects($this->never())
            ->method('getComposite');
        $mockTripodUpdates->expects($this->once())
            ->method('getDiscoverImpactedSubjects')
            ->will($this->returnValue($mockDiscoverImpactedSubjects));

        $mockTripodUpdates->expects($this->once())
            ->method('storeChanges')
            ->will($this->returnValue($subjectsAndPredicatesOfChange));

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockDiscoverImpactedSubjects->expects($this->once())
            ->method('createJob')
            ->with(
                $jobData,
                \Tripod\Mongo\Config::getDiscoverQueueName()
            );

        $mockTripod->saveChanges(new \Tripod\ExtendedGraph(), $oG,"http://talisaspire.com/");
    }

    public function testDiscoverImpactedSubjectsForDeletionsSyncOpsAreDoneAsyncJobSubmitted()
    {
        $uri_1 = "http://example.com/1";
        $uri_2 = "http://example.com/2";
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri_1, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_resource_triple($uri_2, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
//        // just deletes, search only
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array('getDataUpdater','getComposite'),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    'async'=>array(OP_TABLES=>false,OP_VIEWS=>false,OP_SEARCH=>true)
                )
            )
        );

        $mockTripodUpdates = $this->getMock(
            '\Tripod\Mongo\Updates',
            array(
                'storeChanges',
                'getDiscoverImpactedSubjects'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>false,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockViews = $this->getMock(
            '\Tripod\Mongo\Composites\Views',
            array('getImpactedSubjects', 'update'),
            array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing','CBD_testing'),
                'http://talisaspire.com/'
            )
        );

        $mockTables = $this->getMock(
            '\Tripod\Mongo\Composites\Tables',
            array('getImpactedSubjects', 'update'),
            array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing','CBD_testing'),
                'http://talisaspire.com/'
            )
        );

        $mockDiscoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('createJob'))
            ->getMock();

        $labeller = new \Tripod\Mongo\Labeller();

        // The predicates should be empty arrays, since these were deletes
        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias('http://example.com/1')=>array(),
            $labeller->uri_to_alias('http://example.com/2')=>array()
        );

        $jobData = array(
            'changes'=>$subjectsAndPredicatesOfChange,
            'operations'=>array(OP_SEARCH),
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
            'storeName'=>'tripod_php_testing',
            'podName'=>'CBD_testing',
            'contextAlias'=>'http://talisaspire.com/'
        );

        $impactedViewSubjects = array(
            $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>'http://example.com/1',
                            _ID_CONTEXT=>'http://talisaspire.com',
                        ),
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing'
                    )
                )
                ->setMethods(array('update'))
                ->getMock(),
            $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>'http://example.com/2',
                            _ID_CONTEXT=>'http://talisaspire.com',
                        ),
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing'
                    )
                )
                ->setMethods(array('update'))
                ->getMock(),
            $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>'http://example.com/9',
                            _ID_CONTEXT=>'http://talisaspire.com',
                        ),
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing'
                    )
                )
                ->setMethods(array('update'))
                ->getMock()
        );
        $mockViews->expects($this->once())
            ->method('getImpactedSubjects')
            ->will($this->returnValue($impactedViewSubjects));

        $impactedViewSubjects[0]->expects($this->once())->method('update');
        $impactedViewSubjects[1]->expects($this->once())->method('update');
        $impactedViewSubjects[2]->expects($this->once())->method('update');

        // This shouldn't be called because ImpactedSubject->update has been mocked and isn't doing anything
        $mockViews->expects($this->never())->method('update');

        $impactedTableSubjects = array(
            $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>'http://example.com/1',
                            _ID_CONTEXT=>'http://talisaspire.com',
                        ),
                        OP_TABLES,
                        'tripod_php_testing',
                        'CBD_testing'
                    )
                )
                ->setMethods(array('update'))
                ->getMock(),
            $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>'http://example.com/2',
                            _ID_CONTEXT=>'http://talisaspire.com',
                        ),
                        OP_TABLES,
                        'tripod_php_testing',
                        'CBD_testing'
                    )
                )
                ->setMethods(array('update'))
                ->getMock()
        );

        $mockTables->expects($this->once())
            ->method('getImpactedSubjects')
            ->will($this->returnValue($impactedTableSubjects));

        $impactedTableSubjects[0]->expects($this->once())->method('update');
        $impactedTableSubjects[1]->expects($this->once())->method('update');

        $mockTables->expects($this->never())->method('update');

        $mockTripodUpdates->expects($this->once())
            ->method('getDiscoverImpactedSubjects')
            ->will($this->returnValue($mockDiscoverImpactedSubjects));

        $mockTripodUpdates->expects($this->once())
            ->method('storeChanges')
            ->will($this->returnValue($subjectsAndPredicatesOfChange));

        $mockTripod->expects($this->exactly(2))
            ->method('getComposite')
            ->will($this->returnValueMap(
                array(
                    array(OP_TABLES, $mockTables),
                    array(OP_VIEWS, $mockViews)
                )
            ));

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockDiscoverImpactedSubjects->expects($this->once())
            ->method('createJob')
            ->with(
                $jobData,
                \Tripod\Mongo\Config::getDiscoverQueueName()
            );

        $mockTripod->saveChanges($oG, new \Tripod\ExtendedGraph(),"http://talisaspire.com/");
    }

    public function testDiscoverImpactedSubjectsForDefaultOperationsSetting()
    {
        $uri_1 = "http://example.com/1";
        $uri_2 = "http://example.com/2";
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri_1, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_resource_triple($uri_2, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));

        // a delete and an update
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($oG);
        $nG->add_literal_triple($uri_1, $nG->qname_to_uri("searchterms:title"), "wibble");
        $nG->remove_resource_triple($uri_2, $oG->qname_to_uri("rdf:type"), "http://foo/bar#Class2");

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array(
                'getComposite',
                'getDataUpdater'
            ),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array('defaultContext'=>'http://talisaspire.com/')
            )
        );

        $mockTripodUpdates = $this->getMock(
            '\Tripod\Mongo\Updates',
            array(
                'storeChanges',
                'getDiscoverImpactedSubjects'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )

        );

        $mockViews = $this->getMock(
            '\Tripod\Mongo\Composites\Views',
            array('getImpactedSubjects', 'update'),
            array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing','CBD_testing'),
                'http://talisaspire.com/'
            )
        );

        $mockDiscoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('createJob'))
            ->getMock();

        $impactedViewSubjects = array(
            $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>$uri_1,
                            _ID_CONTEXT=>'http://talisaspire.com',
                        ),
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing'
                    )
                )
                ->setMethods(array('update'))
                ->getMock(),
            $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>$uri_2,
                            _ID_CONTEXT=>'http://talisaspire.com',
                        ),
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing'
                    )
                )
                ->setMethods(array('update'))
                ->getMock()
        );

        $labeller = new \Tripod\Mongo\Labeller();

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri_1)=>array('searchterms:title'),
            $labeller->uri_to_alias($uri_2)=>array('rdf:type')
        );

        $jobData = array(
            'changes'=>$subjectsAndPredicatesOfChange,
            'operations'=>array(OP_TABLES, OP_SEARCH),
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
            'storeName'=>'tripod_php_testing',
            'podName'=>'CBD_testing',
            'contextAlias'=>'http://talisaspire.com/'
        );

        // getComposite() should only be called if there are synchronous operations
        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($mockViews));

        $mockTripodUpdates->expects($this->once())
            ->method('getDiscoverImpactedSubjects')
            ->will($this->returnValue($mockDiscoverImpactedSubjects));

        $mockTripodUpdates->expects($this->once())
            ->method('storeChanges')
            ->will($this->returnValue($subjectsAndPredicatesOfChange));

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockViews->expects($this->once())
            ->method('getImpactedSubjects')
            ->will($this->returnValue($impactedViewSubjects));

        $impactedViewSubjects[0]->expects($this->once())->method('update');
        $impactedViewSubjects[1]->expects($this->once())->method('update');

        // This shouldn't be called because ImpactedSubject->update has been mocked and isn't doing anything
        $mockViews->expects($this->never())->method('update');

        $mockDiscoverImpactedSubjects->expects($this->once())
            ->method('createJob')
            ->with(
                $jobData,
                \Tripod\Mongo\Config::getDiscoverQueueName()
            );

        $mockTripod->saveChanges($oG,$nG,"http://talisaspire.com/");
    }

    public function testSpecifyQueueForAsyncOperations()
    {
        $uri_1 = "http://example.com/1";
        $uri_2 = "http://example.com/2";
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_resource_triple($uri_1, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_resource_triple($uri_2, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));

        // a delete and an update
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($oG);
        $nG->add_literal_triple($uri_1, $nG->qname_to_uri("searchterms:title"), "wibble");
        $nG->remove_resource_triple($uri_2, $oG->qname_to_uri("rdf:type"), "http://foo/bar#Class2");

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array(
                'getComposite',
                'getDataUpdater'
            ),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array('defaultContext'=>'http://talisaspire.com/')
            )
        );

        $queueName = 'TRIPOD_TESTING_QUEUE_' . uniqid();

        $mockTripodUpdates = $this->getMock(
            '\Tripod\Mongo\Updates',
            array(
                'storeChanges',
                'getDiscoverImpactedSubjects'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true,
                        'queue'=>$queueName
                    )
                )
            )

        );

        $mockViews = $this->getMock(
            '\Tripod\Mongo\Composites\Views',
            array('getImpactedSubjects', 'update'),
            array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing','CBD_testing'),
                'http://talisaspire.com/'
            )
        );

        $mockDiscoverImpactedSubjects = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(array('createJob'))
            ->getMock();

        $impactedViewSubjects = array(
            $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>$uri_1,
                            _ID_CONTEXT=>'http://talisaspire.com',
                        ),
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing'
                    )
                )
                ->setMethods(array('update'))
                ->getMock(),
            $this->getMockBuilder('\Tripod\Mongo\ImpactedSubject')
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>$uri_2,
                            _ID_CONTEXT=>'http://talisaspire.com',
                        ),
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing'
                    )
                )
                ->setMethods(array('update'))
                ->getMock()
        );

        $labeller = new \Tripod\Mongo\Labeller();

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri_1)=>array('searchterms:title'),
            $labeller->uri_to_alias($uri_2)=>array('rdf:type')
        );

        $jobData = array(
            'changes'=>$subjectsAndPredicatesOfChange,
            'operations'=>array(OP_TABLES, OP_SEARCH),
            'tripodConfig'=>\Tripod\Mongo\Config::getConfig(),
            'storeName'=>'tripod_php_testing',
            'podName'=>'CBD_testing',
            'contextAlias'=>'http://talisaspire.com/',
            'queue'=>$queueName
        );

        // getComposite() should only be called if there are synchronous operations
        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($mockViews));

        $mockTripodUpdates->expects($this->once())
            ->method('getDiscoverImpactedSubjects')
            ->will($this->returnValue($mockDiscoverImpactedSubjects));

        $mockTripodUpdates->expects($this->once())
            ->method('storeChanges')
            ->will($this->returnValue($subjectsAndPredicatesOfChange));

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockViews->expects($this->once())
            ->method('getImpactedSubjects')
            ->will($this->returnValue($impactedViewSubjects));

        // This shouldn't be called because ImpactedSubject->update has been mocked and isn't doing anything
        $mockViews->expects($this->never())->method('update');

        $impactedViewSubjects[0]->expects($this->once())->method('update');
        $impactedViewSubjects[1]->expects($this->once())->method('update');

        $mockDiscoverImpactedSubjects->expects($this->once())
            ->method('createJob')
            ->with(
                $jobData,
                $queueName
            );

        $mockTripod->saveChanges($oG,$nG,"http://talisaspire.com/");
    }

    public function testWriteToUnconfiguredCollectionThrowsException()
    {
//        Exception: testing:SOME_COLLECTION is not referenced within config, so cannot be written to
        $this->setExpectedException(
            '\Tripod\Exceptions\ConfigException',
            'Collection name \'SOME_COLLECTION\' not in configuration');

        $tripod = new \Tripod\Mongo\Driver("SOME_COLLECTION","tripod_php_testing");
        $tripod->saveChanges(new \Tripod\ExtendedGraph(), new \Tripod\ExtendedGraph(), 'http://talisaspire.com/');
    }

    // NAMESPACE TESTS

    /**
     * this test verifies that if we simply want to add some data to a document that exists in we dont need to specify an oldgraph; we just need to specify the new graph
     * the cs builder should translate that into a single addition statement and apply it.
     * This builds on the previous test, by operating on data in mongo where _id.r and _id.c are namespaced
     */
    public function testTripodSaveChangesAddsLiteralTripleUsingEmptyOldGraphWithNamespacableIDAndContext()
    {
        $oG = new \Tripod\Mongo\MongoGraph();
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($oG);

        // resource and context are namespaced in base data this time around...
        $nG->add_literal_triple('http://basedata.com/b/1', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges($oG, $nG,"http://basedata.com/b/DefaultGraph", 'my changes');
        $uG = $this->tripod->describeResource('http://basedata.com/b/1',"http://basedata.com/b/DefaultGraph");
        $this->assertHasLiteralTriple($uG, 'http://basedata.com/b/1', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');
    }

    /**
     * this test verifies that if we simply want to add some data to a document that exists in we dont need to specify an oldgraph; we just need to specify the new graph
     * the cs builder should translate that into a single addition statement and apply it.
     * This builds on the previous test, by operating on data in mongo where _id.r and _id.c are namespaced AND passing context into the save method
     */
    public function testTripodSaveChangesAddsLiteralTripleUsingEmptyOldGraphWithNamespacedContext()
    {
        $oG = new \Tripod\Mongo\MongoGraph();
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($oG);

        // resource and context are namespaced in base data this time around...
        $nG->add_literal_triple('http://basedata.com/b/1', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges($oG, $nG,"baseData:DefaultGraph", 'my changes');
        $uG = $this->tripod->describeResource('http://basedata.com/b/1',"http://basedata.com/b/DefaultGraph");
        $this->assertHasLiteralTriple($uG, 'http://basedata.com/b/1', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');
    }

    public function testDescribeResourceWithNamespace()
    {
        $noNsG = $this->tripod->describeResource('http://basedata.com/b/1',"http://basedata.com/b/DefaultGraph");
        $nsResourceG = $this->tripod->describeResource('baseData:1',"http://basedata.com/b/DefaultGraph");
        $nsContextG = $this->tripod->describeResource('http://basedata.com/b/1',"baseData:DefaultGraph");
        $nsBothG = $this->tripod->describeResource('baseData:1',"baseData:DefaultGraph");

        $this->assertEquals($noNsG->to_rdfxml(),$nsResourceG->to_rdfxml(),"Non ns and nsResource not equal");
        $this->assertEquals($noNsG->to_rdfxml(),$nsContextG->to_rdfxml(),"Non ns and nsContext not equal");
        $this->assertEquals($noNsG->to_rdfxml(),$nsBothG->to_rdfxml(),"Non ns and nsBoth not equal");
    }

    public function testDescribeResourcesWithNamespace()
    {
        $noNsG = $this->tripod->describeResources(array('http://basedata.com/b/1'),"http://basedata.com/b/DefaultGraph");
        $nsResourceG = $this->tripod->describeResources(array('baseData:1'),"http://basedata.com/b/DefaultGraph");
        $nsContextG = $this->tripod->describeResources(array('http://basedata.com/b/1'),"baseData:DefaultGraph");
        $nsBothG = $this->tripod->describeResources(array('baseData:1'),"baseData:DefaultGraph");

        $this->assertEquals($noNsG->to_rdfxml(),$nsResourceG->to_rdfxml(),"Non ns and nsResource not equal");
        $this->assertEquals($noNsG->to_rdfxml(),$nsContextG->to_rdfxml(),"Non ns and nsContext not equal");
        $this->assertEquals($noNsG->to_rdfxml(),$nsBothG->to_rdfxml(),"Non ns and nsBoth not equal");
    }

    public function testSelectSingleValueWithNamespaceContextQueryDoesntContainID()
    {
        $expectedResult = array(
            "head" => array (
                "count"=>2,
                "offset"=>0,
                "limit"=>NULL
            ),
            "results"=> array(
                array(
                    "_id"=>array(
                        _ID_RESOURCE=>"baseData:1",
                        _ID_CONTEXT=>"baseData:DefaultGraph"),
                    "rdf:type"=>"acorn:Work"
                ),
                array(
                    "_id"=>array(
                        _ID_RESOURCE=>"baseData:2",
                        _ID_CONTEXT=>"baseData:DefaultGraph"),
                    "rdf:type"=>array("acorn:Work","acorn:Work2")
                )
            )
        );
        $actualResult = $this->tripod->select(array("rdf:type.".VALUE_URI=>"acorn:Work"),array("rdf:type"=>true),null,null,0,"baseData:DefaultGraph");
        $this->assertEquals($expectedResult,$actualResult);
    }

    public function testSelectSingleValueWithNamespaceContextQueryDoesContainID()
    {
        $expectedResult = array(
            "head" => array (
                "count"=>1,
                "offset"=>0,
                "limit"=>NULL
            ),
            "results"=> array(
                array(
                    "_id"=>array(
                        _ID_RESOURCE=>"baseData:1",
                        _ID_CONTEXT=>"baseData:DefaultGraph"),
                    "rdf:type"=>"acorn:Work"
                )
            )
        );
        $actualResult = $this->tripod->select(
            array("_id"=>array(_ID_RESOURCE=>"baseData:1")),
            array("rdf:type"=>true),
            null,
            null,
            0,
            "baseData:DefaultGraph");
        $this->assertEquals($expectedResult,$actualResult);
    }

    public function testSelectWithOperandWithNamespaceContextQueryContainsID()
    {
        $expectedResult = array(
            "head" => array (
                "count"=>2,
                "offset"=>0,
                "limit"=>NULL
            ),
            "results"=> array(
                array(
                    "_id"=>array(
                        _ID_RESOURCE=>"baseData:1",
                        _ID_CONTEXT=>"baseData:DefaultGraph"),
                    "rdf:type"=>"acorn:Work"
                ),
                array(
                    "_id"=>array(
                        _ID_RESOURCE=>"baseData:2",
                        _ID_CONTEXT=>"baseData:DefaultGraph"),
                    "rdf:type"=>array("acorn:Work","acorn:Work2")
                )            )
        );
        $actualResult = $this->tripod->select(
            array("_id"=>array('$in'=>array(array(_ID_RESOURCE=>"baseData:1"),array(_ID_RESOURCE=>"baseData:2")))),
            array("rdf:type"=>true),
            null,
            null,
            0,
            "baseData:DefaultGraph");
        $this->assertEquals($expectedResult,$actualResult);

    }
    public function testSelectWithOperandWithNamespaceContextQueryDoesNotContainID()
    {
        $expectedResult = array(
            "head" => array (
                "count"=>2,
                "offset"=>0,
                "limit"=>NULL
            ),
            "results"=> array(
                array(
                    "_id"=>array(
                        _ID_RESOURCE=>"baseData:1",
                        _ID_CONTEXT=>"baseData:DefaultGraph"),
                    "rdf:type"=>"acorn:Work"
                ),
                array(
                    "_id"=>array(
                        _ID_RESOURCE=>"baseData:2",
                        _ID_CONTEXT=>"baseData:DefaultGraph"),
                    "rdf:type"=>array("acorn:Work","acorn:Work2")
                )
            )
        );
        $actualResult = $this->tripod->select(
            array("rdf:type"=>array('$in'=>array(array(VALUE_URI=>"acorn:Work")))),
            array("rdf:type"=>true),
            null,
            null,
            0,
            "baseData:DefaultGraph");
        $this->assertEquals($expectedResult,$actualResult);
    }

    /**
     * Return the distinct values of a table column
     * @access public
     * @return void
     */
    public function testGetDistinctTableValues()
    {
        // Get table rows
        $table = 't_distinct';
        $this->tripod->generateTableRows($table);
        $rows = $this->tripod->getTableRows($table, array(), array(), 0, 0);
        $this->assertEquals(7, $rows['head']['count']);
        $results = $this->tripod->getDistinctTableColumnValues($table, "value.title");

        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(3, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEquals(3, count($results['results']));
        $this->assertContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        // Supply a filter
        $results = $this->tripod->getDistinctTableColumnValues($table, "value.title", array('value.type'=>"bibo:Document"));
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(2, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEquals(2, count($results['results']));
        $this->assertNotContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        $results = $this->tripod->getDistinctTableColumnValues($table, "value.type");
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(4, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEquals(4, count($results['results']));
        $this->assertContains('acorn:Resource', $results['results']);
        $this->assertContains('acorn:Work', $results['results']);
        $this->assertContains('bibo:Book', $results['results']);
        $this->assertContains('bibo:Document', $results['results']);
    }

    /**
     * Return no results for tablespec that doesn't exist
     * @access public
     * @return void
     */
    public function testDistinctOnTableSpecThatDoesNotExist()
    {
        $table = "t_nothing_to_see_here";

        $this->setExpectedException(
            '\Tripod\Exceptions\ConfigException',
            'Table id \'t_nothing_to_see_here\' not in configuration'
        );
        $results = $this->tripod->getDistinctTableColumnValues($table, "value.foo");

    }

    /**
     * Return no results for distinct on a fieldname that is not defined in tableSpec
     * @access public
     * @return void
     */
    public function testDistinctOnFieldNameThatIsNotInTableSpec()
    {
        // Get table rows
        $table = 't_distinct';
        $this->tripod->generateTableRows($table);
        $results = $this->tripod->getDistinctTableColumnValues($table, "value.foo");
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    /**
     * Return no results for filters that match no table rows
     * @access public
     * @return void
     */
    public function testDistinctForFilterWithNoMatches()
    {
        // Get table rows
        $table = 't_distinct';
        $this->tripod->generateTableRows($table);
        $results = $this->tripod->getDistinctTableColumnValues($table, "value.title", array('value.foo'=>"wibble"));
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    /**  START: getLockedDocuments tests */

    public function testGetLockedDocuments()
    {
        $subject = "http://talisaspire.com/works/lockedDoc";
        $this->lockDocument($subject,"transaction_100");

        $docs = $this->tripod->getLockedDocuments();
        $this->assertEquals(1, count($docs));
        $this->assertEquals($docs[0]['_id']['r'],$subject);
        $this->assertEquals($docs[0][_LOCKED_FOR_TRANS],"transaction_100");
    }

    public function testGetLockedDocumentsWithFromDateOnly()
    {
        $subject = "http://talisaspire.com/works/lockedDoc";
        $this->lockDocument($subject,"transaction_100");

        $docs = $this->tripod->getLockedDocuments(date("y-m-d H:i:s", strtotime("+1 min")));
        $this->assertEquals(0, count($docs));

        $docs = $this->tripod->getLockedDocuments(date("y-m-d H:i:s", strtotime("-1 min")));
        $this->assertEquals(1, count($docs));
    }

    public function testGetLockedDocumentsWithTillDateOnly()
    {
        $subject = "http://talisaspire.com/works/lockedDoc";
        $this->lockDocument($subject,"transaction_100");

        $docs = $this->tripod->getLockedDocuments(null, date("y-m-d H:i:s", strtotime("+1 min")));
        $this->assertEquals(1, count($docs));

        $docs = $this->tripod->getLockedDocuments(null, date("y-m-d H:i:s"), strtotime("-1 min"));
        $this->assertEquals(0, count($docs));
    }

    public function testGetLockedDocumentsWithDateRange()
    {
        $subject = "http://talisaspire.com/works/lockedDoc";
        $this->lockDocument($subject,"transaction_100");

        $docs = $this->tripod->getLockedDocuments(date("y-m-d H:i:s", strtotime("-1 min")), date("y-m-d H:i:s", strtotime("+1 min")));
        $this->assertEquals(1, count($docs));

        $docs = $this->tripod->getLockedDocuments(date("y-m-d H:i:s", strtotime("+1 min")), date("y-m-d H:i:s", strtotime("+2 min")));
        $this->assertEquals(0, count($docs));
    }

    /** END: getLockedDocuments tests */

    /**  START: removeInertLocks tests */
    public function testRemoveInertLocksNoLocksFound()
    {
        $this->assertFalse($this->tripod->removeInertLocks("transaction_100", "Unit tests"));
    }

    public function testRemoveInertLocksNotAllLocksAreRemoved()
    {
        $subjectOne = "http://talisaspire.com/works/lockedDoc";
        $subjectTwo = "http://basedata.com/b/1";

        $this->lockDocument($subjectOne,"transaction_500");
        $this->lockDocument($subjectTwo,"transaction_200");

        $docs = $this->tripod->getLockedDocuments();
        $this->assertEquals(2, count($docs));

        $this->tripod->removeInertLocks("transaction_200", "Unit tests");
        $docs = $this->tripod->getLockedDocuments();
        $this->assertEquals(1, count($docs));
    }

    public function testRemoveInertLocksCreateAuditEntryThrowsException()
    {
        $subject = "http://basedata.com/b/1";
        $this->lockDocument($subject,"transaction_400");

        $this->setExpectedException('Exception', 'Some unexpected error occurred.');

        /* @var $auditManualRollbackCollection PHPUnit_Framework_MockObject_MockObject */
        $auditManualRollbackCollection = $this->getMock("MongoCollection", array('insert'), array(), '', false);
        $auditManualRollbackCollection->expects($this->once())
            ->method('insert')
            ->will($this->throwException(new Exception('Some unexpected error occurred.')));

        /* @var $tripod PHPUnit_Framework_MockObject_MockObject */
        $tripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripodUpdate = $this->getMock('\Tripod\Mongo\Updates', array('getAuditManualRollbacksCollection'), array($tripod));

        $tripodUpdate
            ->expects($this->once())
            ->method("getAuditManualRollbacksCollection")
            ->will($this->returnValue($auditManualRollbackCollection));

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $tripod->removeInertLocks("transaction_400", "Unit tests");
    }

    public function testRemoveInertLocksUnlockAllDocumentsFailsVerifyErrorEntryInAuditLog()
    {
        $subject = "http://basedata.com/b/1";
        $this->lockDocument($subject,"transaction_400");

        $mongoDocumentId = new MongoId();
        $mongoDate = new MongoDate();

        $this->setExpectedException('Exception', 'Some unexpected error occurred.');

        /* @var $auditManualRollbackCollection PHPUnit_Framework_MockObject_MockObject */
        $auditManualRollbackCollection = $this->getMock("MongoCollection", array('update', 'insert'), array(), '', false);

        $auditManualRollbackCollection->expects($this->once())
            ->method('insert')
            ->will($this->returnValue(array("ok" => true, 'err'=>NULL)));

        $auditManualRollbackCollection->expects($this->once())
            ->method('update')
            ->with(array("_id" => $mongoDocumentId), array('$set' => array("status" => AUDIT_STATUS_ERROR, _UPDATED_TS => $mongoDate, 'error' => 'Some unexpected error occurred.')));

        /* @var $tripod PHPUnit_Framework_MockObject_MockObject */
        $tripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripodUpdate = $this->getMock('\Tripod\Mongo\Updates',
            array('unlockAllDocuments', 'generateIdForNewMongoDocument', 'getMongoDate', 'getAuditManualRollbacksCollection'),
            array($tripod));
        
        $tripodUpdate->expects($this->once())
            ->method("generateIdForNewMongoDocument")
            ->will($this->returnValue($mongoDocumentId));

        $tripodUpdate->expects($this->exactly(2))
            ->method("getMongoDate")
            ->will($this->returnValue($mongoDate));

        $tripodUpdate->expects($this->once())
            ->method("getAuditManualRollbacksCollection")
            ->will($this->returnValue($auditManualRollbackCollection));

        $tripodUpdate->expects($this->once())
            ->method("unlockAllDocuments")
            ->will($this->throwException(new Exception('Some unexpected error occurred.')));

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $tripod->removeInertLocks("transaction_400", "Unit tests");
    }

    public function testRemoveInertLocksUnlockSuccessfulVerifyAuditLog()
    {
        $subject = "http://basedata.com/b/1";
        $subject2 = "tenantData:1";
        $this->lockDocument($subject,"transaction_400");
        $this->lockDocument($subject2,"transaction_400");

        $mongoDocumentId = new MongoId();
        $mongoDate = new MongoDate();

        /* @var $auditManualRollbackCollection PHPUnit_Framework_MockObject_MockObject */
        $auditManualRollbackCollection = $this->getMock("MongoCollection", array('insert','update'), array(), '', false);

        $auditManualRollbackCollection->expects($this->once())
            ->method('insert')
            ->with(array(
                '_id' => $mongoDocumentId,
                'type' => AUDIT_TYPE_REMOVE_INERT_LOCKS,
                'status' => AUDIT_STATUS_IN_PROGRESS,
                'reason' => "Unit tests",
                'transaction_id' => "transaction_400",
                'documents' => array("baseData:1", "tenantData:1"),
                _CREATED_TS=> $mongoDate,
            ))
            ->will($this->returnValue(array("ok" => true, 'err'=>NULL)));

        $auditManualRollbackCollection->expects($this->once())
            ->method('update')
            ->with(array("_id" => $mongoDocumentId), array('$set' => array("status" => AUDIT_STATUS_COMPLETED, _UPDATED_TS => $mongoDate)));

        /* @var \Tripod\Mongo\Driver PHPUnit_Framework_MockObject_MockObject */
        $tripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripodUpdate = $this->getMock('\Tripod\Mongo\Updates',
            array('unlockAllDocuments', 'generateIdForNewMongoDocument', 'getMongoDate', 'getAuditManualRollbacksCollection'),
            array($tripod));
        
        $tripodUpdate->expects($this->once())
            ->method("generateIdForNewMongoDocument")
            ->will($this->returnValue($mongoDocumentId));

        $tripodUpdate->expects($this->once())
            ->method("getAuditManualRollbacksCollection")
            ->will($this->returnValue($auditManualRollbackCollection));

        $tripodUpdate->expects($this->exactly(2))
            ->method("getMongoDate")
            ->will($this->returnValue($mongoDate));

        $tripodUpdate->expects($this->once())
            ->method("unlockAllDocuments")
            ->will($this->returnValue(true));

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $this->assertTrue($tripod->removeInertLocks("transaction_400", "Unit tests"));
    }

    public function testRemoveInertLocks()
    {
        $subject = "http://basedata.com/b/1";
        $this->lockDocument($subject,"transaction_100");

        $this->tripod->removeInertLocks("transaction_100", "Unit tests");
        $docs = $this->tripod->getLockedDocuments();
        $this->assertEquals(0, count($docs));
    }

    public function testStatsD() {
        $mockStatsD = $this->getMock('\Tripod\StatsD', array('send'), array("localhost","2012","myapp.tripod.group_by_db.tripod_php_testing"));

        /* @var $mockTripod \Tripod\Mongo\Driver PHPUnit_Framework_MockObject_MockObject */
        $mockTripod = $this->getMock('\Tripod\Mongo\Driver', array('getStat'), array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/',"statsDHost"=>"localhost","statsDPort"=>"2012","statsDPrefix"=>"myapp")));
        $mockTripod->expects($this->any())
            ->method("getStat")
            ->will($this->returnValue($mockStatsD));

        $mockStatsD->expects($this->once())
            ->method("send")
            ->with(array("myapp.tripod.group_by_db.tripod_php_testing.MONGO_GET_ETAG"=>"1|c"));

        $mockTripod->getETag("http://foo");
    }


    /** END: removeInertLocks tests */
}
