<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoTripod.class.php';

class MongoTripodTest extends MongoTripodTestBase
{
    /**
     * @var MongoTripod | PHPUnit_Framework_MockObject_MockObject
     */
    protected $tripod = null;
    /**
     * @var MongoTransactionLog
     */
    protected $tripodTransactionLog = null;

    protected function setUp()
    {
        parent::setup();

        $this->tripodTransactionLog = new MongoTransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        // Stub ouf 'addToElastic' search to prevent writes into Elastic Search happening by default.
        $this->tripod = $this->getMock('MongoTripod', array('addToSearchIndexQueue'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $this->tripod->expects($this->any())->method('addToSearchIndexQueue');

        $this->tripod->collection->drop();

        // Lock collection no longer available from MongoTripod, so drop it manually
        $this->tripod->db->selectCollection(LOCKS_COLLECTION)->drop();

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
        $nG = new MongoGraph();
        $nG->add_graph($oG);
        $nG->remove_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition');

        $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/",'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $this->assertEquals($nG, $uG, "Updated does not match expected graph");
    }

    public function testTripodSaveChangesAddsLiteralTriple()
    {
        $oG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $nG = new MongoGraph();
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
        $oG = new MongoGraph();
        $oG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $oG->qname_to_uri("bibo:isbn13"), "9780393929690");
        $nG = new MongoGraph();

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
        $oG = new MongoGraph();
        $nG = new MongoGraph();
        $nG->add_graph($oG);
        $nG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/", 'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $this->assertHasLiteralTriple($uG, 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');
    }

    public function testTripodSaveChangesUpdatesLiteralTriple()
    {
        $oG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $nG = new MongoGraph();
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

        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");
        $this->tripod->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/", "something new");

        $uG = $this->tripod->describeResource($uri);

        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
    }

    public function testRemoveGraphEntirely()
    {
        $uri = 'http://example.com/resources/1';

        // create a new entity and save it
        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");
        $this->tripod->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/", "something new");

        // retrieve it and make sure it was saved correctly
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");

        // now remove all knowledge about it, then describe the resource again, should be an empty graph
        $this->tripod->saveChanges($uG, new MongoGraph(),"http://talisaspire.com/", "murder death kill");
        $g = $this->tripod->describeResource($uri);

        $this->assertTrue($g->is_empty());
    }

    public function testSaveFailsWhenOldGraphIsInvalidNoDataInStoreForObj()
    {
        $this->setExpectedException('TripodException', 'Error storing changes');

        $uri = 'http://example.com/resources/1';

        // create a new entity and save it
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));

        $nG = new MongoGraph();
        $nG->add_resource_triple($uri, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:List"));

        $result = $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/");
    }

    public function testInterleavingUpdateFailsIfUnderlyingDataHasChanged()
    {
        $this->setExpectedException('TripodException', 'Error storing changes');

        $uri = 'http://example.com/resources/1';

        $g = new MongoGraph();
        // canned response will simulate that the underlying data has changed
        $doc = array("_id"=>$uri, "rdf:type"=>array(array('value'=>$g->qname_to_uri("acorn:Resource"), 'type'=>'uri')));

        $mockTripod = $this->getMock('MongoTripod', array('getDataUpdateManager'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));

        $mockTripodUpdate = $this->getMock('MongoTripodUpdates', array('getDocumentForUpdate'), array($mockTripod));
        $mockTripodUpdate->expects($this->once())->method('getDocumentForUpdate')->with($uri)->will($this->returnValue($doc));
        $mockTripod->expects($this->once())
            ->method('getDataUpdateManager')
            ->will($this->returnValue($mockTripodUpdate));
        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Book"));
        $nG = new MongoGraph();
        $nG->add_resource_triple($uri, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:Foo"));

        $result = $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
    }

    public function testInterleavingUpdateFailsIfCriteriaIsNotValidAtPointOfSave()
    {
        $this->setExpectedException('TripodException', 'Error storing changes');

        // save some data in the store
        $uri = 'http://example.com/resources/1';

        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Book"));
        $this->tripod->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/");

        // canned response will simulate that the underlying data has changed
        $doc = array("_id"=>$uri, "_version"=>3,"rdf:type"=>array(array('value'=>$g->qname_to_uri("acorn:Resource"), 'type'=>'uri')));

        $mockTripod = $this->getMock('MongoTripod', array('getDataUpdateManager'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));

        $mockTripodUpdate = $this->getMock('MongoTripodUpdates', array('getDocumentForUpdate'), array($mockTripod));
        $mockTripodUpdate->expects($this->once())->method('getDocumentForUpdate')->with($uri)->will($this->returnValue($doc));
        $mockTripod->expects($this->once())
            ->method('getDataUpdateManager')
            ->will($this->returnValue($mockTripodUpdate));
        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));

        $nG = new MongoGraph();
        $nG->add_resource_triple($uri, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:Foo"));

        $result = $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
    }

    public function testAddMultipleTriplesForSameProperty()
    {
        $uri = 'http://example.com/resources/1';

        // save a graph to the store
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Some title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Another title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Yet another title");
        $this->tripod->saveChanges(new MongoGraph(), $oG,"http://talisaspire.com/");

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
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Some title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Another title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Yet another title");
        $this->tripod->saveChanges(new MongoGraph(), $oG,"http://talisaspire.com/");

        // remove all three dct:title triples
        $g2 = new MongoGraph();
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
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Some title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Another title");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Yet another title");
        $this->tripod->saveChanges(new MongoGraph(), $oG,"http://talisaspire.com/");

        $g2= new MongoGraph();
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
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title one");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title two");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title three");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title four");
        $oG->add_literal_triple($uri, $oG->qname_to_uri("dct:title"), "Title five");
        $this->tripod->saveChanges(new MongoGraph(), $oG,"http://talisaspire.com/");

        // new data
        $g2= new MongoGraph();
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
        /** @var $tripodMock MongoTripod **/
        $tripodMock = $this->getMock('MongoTripod', array('getDataUpdateManager'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripodUpdate = $this->getMock('MongoTripodUpdates',
            array('addToSearchIndexQueue','setReadPreferenceToPrimary','resetOriginalReadPreference'), array($tripodMock));
        $tripodUpdate
            ->expects($this->once(0))
            ->method('setReadPreferenceToPrimary');

        $tripodUpdate
            ->expects($this->once())
            ->method('resetOriginalReadPreference');
        $tripodMock
            ->expects($this->once())
            ->method('getDataUpdateManager')
            ->will($this->returnValue($tripodUpdate));

        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title"), "Title one");
        $tripodMock->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/");
    }

    /**
     * @expectedException Exception
     */
    public function testReadPreferencesAreRestoredWhenErrorSavingChanges(){
        $subjectOne = "http://talisaspire.com/works/checkReadPreferencesAreRestoredOnError";
        /** @var $tripodMock MongoTripod **/
        $tripodMock = $this->getMock('MongoTripod', array('getDataUpdateManager'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripodUpdate = $this->getMock('MongoTripodUpdates',
            array('addToSearchIndexQueue','resetOriginalReadPreference','getContextAlias'), array($tripodMock));

        $tripodUpdate
            ->expects($this->once())
            ->method('getContextAlias')
            ->will($this->throwException(new Exception("A Test Exception")));

        $tripodUpdate
            ->expects($this->once())
            ->method('resetOriginalReadPreference');

        $tripodMock
            ->expects($this->once())
            ->method('getDataUpdateManager')
            ->will($this->returnValue($tripodUpdate));

        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title"), "Title one");
        $tripodMock->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/");
    }

    public function testReadPreferencesOverMultipleSaves(){
        $subjectOne = "http://talisaspire.com/works/checkReadPreferencesOverMultipleSaves";
        /** @var $tripodMock MongoTripod **/
        $tripodMock = $this->getMock(
            'MongoTripod',
            array('getDataUpdateManager'),
            array('CBD_testing','testing',
                array('defaultContext'=>'http://talisaspire.com/', 'readPreference'=>MongoClient::RP_SECONDARY_PREFERRED))
        );

        $tripodUpdate = $this->getMock('MongoTripodUpdates',
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
            ->method('getDataUpdateManager')
            ->will($this->returnValue($tripodUpdate));

        $expectedReadPreference = $tripodUpdate->getReadPreference();
        $this->assertEquals($expectedReadPreference['type'], MongoClient::RP_SECONDARY_PREFERRED);

        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title"), "Title one");
        $tripodMock->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/");
        $this->assertEquals($expectedReadPreference, $tripodUpdate->getReadPreference());

        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title2"), "Title two");
        $exceptionThrown = false;
        try{
            $tripodMock->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/");
        }
        catch(Exception $e){
            $exceptionThrown = true;
            $this->assertEquals("readPreferenceOverMultipleSavesTestException", $e->getMessage());
        }
        $this->assertTrue($exceptionThrown);
        $this->assertEquals($expectedReadPreference, $tripodUpdate->getReadPreference());

        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title3"), "Title three");
        $tripodMock->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/");
        $this->assertEquals($expectedReadPreference, $tripodUpdate->getReadPreference());
    }

    public function testSaveChangesToLockedDocument()
    {
        $subjectOne = "http://talisaspire.com/works/lockedDoc";

        $this->lockDocument($subjectOne,"transaction_101");

        $this->setExpectedException('TripodException', "Error storing changes: Did not obtain locks on documents");

        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri("dct:title"), "Title one");

        $this->tripod->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/");

    }

    public function testSaveChangesToMultipleSubjects()
    {
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';

        // save a graph to the store containng two completely new entities
        $oG = new MongoGraph();
        $oG->add_resource_triple($subjectOne, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
        $oG->add_literal_triple($subjectOne, $oG->qname_to_uri("dct:title"), "Title one");
        $oG->add_literal_triple($subjectOne, $oG->qname_to_uri("dct:title"), "Title two");
        $oG->add_resource_triple($subjectTwo, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Book"));
        $oG->add_literal_triple($subjectTwo, $oG->qname_to_uri("dct:title"), "Title three");
        $oG->add_literal_triple($subjectTwo, $oG->qname_to_uri("dct:title"), "Title four");
        $this->tripod->saveChanges(new MongoGraph(), $oG,"http://talisaspire.com/");

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
        $nG = new MongoGraph();
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
        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");
        $this->tripod->saveChanges(new MongoGraph(), $g, "http://talisaspire.com/", "something new");
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 0);

        $nG = new MongoGraph();
        $nG->add_graph($g);
        $nG->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "another title");
        $this->tripod->saveChanges($g, $nG,"http://talisaspire.com/");
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "another title"), "Graph should contain literal triple we added");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 1);

        $nG = new MongoGraph();
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
        $this->tripod->saveChanges($nG, new MongoGraph(),"http://talisaspire.com/");
        $this->assertDocumentHasBeenDeleted(array("r"=>$uri,"c"=>"http://talisaspire.com/"));
    }

    public function testSaveChangesWithInvalidCardinality()
    {
        $this->setExpectedException('TripodCardinalityException', "Cardinality failed on http://foo/bar/1 for 'rdf:type' - should only have 1 value and has: http://foo/bar#Class1, http://foo/bar#Class2");

        $config = array();
        $config['namespaces'] = array('rdf'=>'http://www.w3.org/1999/02/22-rdf-syntax-ns#');
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://talisaspire:acorn123@46.137.106.66:27018");
        $config["es_config"] = array("search_document_specifications"=>array(),"indexes"=>array(),"endpoint"=>"http://localhost");
        $config["databases"] = array(
            "testing"=>array(
                "connStr"=>"mongodb://localhost",
                "collections"=>array(
                    "CBD_testing"=>array(
                        "cardinality"=>array(
                            "rdf:type"=>1
                        )
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");

        // Override the config defined in base test class as we need specific config here.
        MongoTripodConfig::setConfig($config);

        $tripod = new MongoTripod('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/'));

        $oldGraph = new ExtendedGraph();
        $newGraph = new ExtendedGraph();
        $newGraph->add_resource_triple('http://foo/bar/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://foo/bar#Class1');
        $newGraph->add_resource_triple('http://foo/bar/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://foo/bar#Class2');

        $tripod->saveChanges($oldGraph, $newGraph,"http://talisaspire.com/");
    }


// TODO: need to completely re-write this test
//    public function testOperationsAreDoneSyncAndASync()
//    {
//        $uri_1 = "http://example.com/1";
//        $uri_2 = "http://example.com/2";
//        $oG = new MongoGraph();
//        $oG->add_resource_triple($uri_1, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
//        $oG->add_resource_triple($uri_2, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
//
//        // just updates, all three operations async
//        $mockTripod = $this->getMock('MongoTripod', array('queueASyncOperations','processSyncOperations'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/','async'=>array(OP_TABLES=>true,OP_VIEWS=>true,OP_SEARCH=>true))));
//        $mockTripod->expects($this->once())->method('queueASyncOperations')->with(array('http://example.com/1', 'http://example.com/2'), array(), 'http://talisaspire.com/', array(OP_TABLES,OP_VIEWS,OP_SEARCH));
//        $mockTripod->expects($this->never())->method('processSyncOperations');
//        $mockTripod->saveChanges(new ExtendedGraph(), $oG,"http://talisaspire.com/");
//
//        // just deletes, search only
//        $mockTripod = $this->getMock('MongoTripod', array('queueASyncOperations','processSyncOperations'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/','async'=>array(OP_TABLES=>false,OP_VIEWS=>false,OP_SEARCH=>true))));
//        $mockTripod->expects($this->once())->method('queueASyncOperations')->with(array(), array('http://example.com/1', 'http://example.com/2'),'http://talisaspire.com/', array(OP_SEARCH));
//        $mockTripod->expects($this->once())->method('processSyncOperations')->with(array(), array('http://example.com/1', 'http://example.com/2'),'http://talisaspire.com/', array(OP_TABLES,OP_VIEWS));
//        $mockTripod->saveChanges($oG, new ExtendedGraph(),"http://talisaspire.com/");
//
//        // add data back into store, default async (should be tables and search)
//        $mockTripod = $this->getMock('MongoTripod', array('queueASyncOperations','processSyncOperations'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
//        $mockTripod->expects($this->once())->method('processSyncOperations')->with(array('http://example.com/1', 'http://example.com/2'), array(),'http://talisaspire.com/', array(OP_VIEWS));
//        $mockTripod->expects($this->once())->method('queueASyncOperations')->with(array('http://example.com/1', 'http://example.com/2'), array(),'http://talisaspire.com/', array(OP_TABLES,OP_SEARCH));
//        $mockTripod->saveChanges(new ExtendedGraph(), $oG,"http://talisaspire.com/");
//
//        // a delete and an update
//        $nG = new MongoGraph();
//        $nG->add_graph($oG);
//        $nG->add_literal_triple($uri_1, $nG->qname_to_uri("searchterms:title"), "wibble");
//        $nG->remove_resource_triple($uri_2, $oG->qname_to_uri("rdf:type"), "http://foo/bar#Class2");
//
//        //default async
//        $mockTripod = $this->getMock('MongoTripod', array('queueASyncOperations','processSyncOperations'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
//        $mockTripod->expects($this->once())->method('queueASyncOperations')->with(array('http://example.com/1'), array('http://example.com/2'),'http://talisaspire.com/', array(OP_TABLES,OP_SEARCH));
////        $mockTripod->expects($this->once())->method('processSyncOperations')->with(array(), array('http://example.com/1', 'http://example.com/2'),'http://talisaspire.com/', array(OP_VIEWS));
//        $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
//
//        //no async
//        $mockTripod = $this->getMock('MongoTripod', array('queueASyncOperations','processSyncOperations'), array('CBD_testing','testing',array('async'=>array(OP_TABLES=>false,OP_VIEWS=>false,OP_SEARCH=>false))));
//        $mockTripod->expects($this->never())->method('queueASyncOperations');
////        $mockTripod->expects($this->once())->method('processSyncOperations')->with(array(), array('http://example.com/1', 'http://example.com/2'),'http://talisaspire.com/', array(OP_VIEWS,OP_TABLES,OP_SEARCH));
//        $mockTripod->saveChanges($nG, $oG,"http://talisaspire.com/");
//    }

    public function testWriteToUnconfiguredCollectionThrowsException()
    {
//        TripodException: testing:SOME_COLLECTION is not referenced within config, so cannot be written to
        $this->setExpectedException(
            'TripodException',
            'database:collection testing:SOME_COLLECTION is not referenced within config, so cannot be written to');

        $tripod = new MongoTripod("SOME_COLLECTION","testing");
        $tripod->saveChanges(new ExtendedGraph(), new ExtendedGraph(), 'http://talisaspire.com/');
    }

    // NAMESPACE TESTS

    /**
     * this test verifies that if we simply want to add some data to a document that exists in we dont need to specify an oldgraph; we just need to specify the new graph
     * the cs builder should translate that into a single addition statement and apply it.
     * This builds on the previous test, by operating on data in mongo where _id.r and _id.c are namespaced
     */
    public function testTripodSaveChangesAddsLiteralTripleUsingEmptyOldGraphWithNamespacableIDAndContext()
    {
        $oG = new MongoGraph();
        $nG = new MongoGraph();
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
        $oG = new MongoGraph();
        $nG = new MongoGraph();
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

// TODO: re-write this test now that operations are defined as part of the ModifiedSubjects that are passed to queueAsync
//    public function testAsyncOperationsDoNotContainSearchIfNoESConfig()
//    {
//        $configFileName = dirname(__FILE__).'/data/config.json';
//        $config = json_decode(file_get_contents($configFileName), true);
//
//        $config['search_config'] = array();
//        MongoTripodConfig::setConfig($config);
//
//        $uri_1 = "http://example.com/1";
//        $oG = new MongoGraph();
//        $oG->add_resource_triple($uri_1, $oG->qname_to_uri("rdf:type"), $oG->qname_to_uri("acorn:Resource"));
//
//        // just updates, all three operations async
//        $mockTripod = $this->getMock('MongoTripod', array('queueASyncOperations','processSyncOperations'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/',"async"=>array(OP_TABLES=>true,OP_VIEWS=>true,OP_SEARCH=>true))));
//        $mockTripod->expects($this->once())->method('queueASyncOperations')->with(array('http://example.com/1'), array(), 'http://talisaspire.com/', array(OP_VIEWS, OP_TABLES));
//        $mockTripod->expects($this->never())->method('processSyncOperations');
//        $mockTripod->saveChanges(new ExtendedGraph(), $oG,"http://talisaspire.com/");
//    }

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
        $rows = $this->tripod->getTableRows($table, array(), array(), 0, 0);
        $this->assertEquals(0, $rows['head']['count']);
        $results = $this->tripod->getDistinctTableColumnValues($table, "value.foo");
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
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
        $tripod = $this->getMock('MongoTripod', array('getDataUpdateManager'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripodUpdate = $this->getMock('MongoTripodUpdates', array('getAuditManualRollbacksCollection'), array($tripod));

        $tripodUpdate
            ->expects($this->once())
            ->method("getAuditManualRollbacksCollection")
            ->will($this->returnValue($auditManualRollbackCollection));

        $tripod->expects($this->once())
            ->method('getDataUpdateManager')
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
        $tripod = $this->getMock('MongoTripod', array('getDataUpdateManager'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripodUpdate = $this->getMock('MongoTripodUpdates',
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
            ->method('getDataUpdateManager')
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

        /* @var MongoTripod PHPUnit_Framework_MockObject_MockObject */
        $tripod = $this->getMock('MongoTripod', array('getDataUpdateManager'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripodUpdate = $this->getMock('MongoTripodUpdates',
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
            ->method('getDataUpdateManager')
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


    /** END: removeInertLocks tests */

//TODO Need to discuss the switch from a db->find() to get types to looking in ExtendedGraph a bit more
//     before enabling this test otherwise we'll have a create another resource that gets loaded into
//     mongo to query or change the code to make the db->find() mockable.
//    public function testSaveChangesCallsGenerateIndexWhenOneType()
//    {
//        $oldGraph = new ExtendedGraph();
//        $newGraph = new ExtendedGraph();
//
//        $oldGraph->add_literal_triple('http://talisaspire.com/foo/1', 'http://talisaspire.com/schema#foo1', 'foo1');
//        $oldGraph->add_resource_triple('http://talisaspire.com/foo/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://purl.org/ontology/bibo/Book');
//
//        $newGraph->add_literal_triple('http://talisaspire.com/foo/1', 'http://talisaspire.com/schema#foo1', 'foo1');
//        $newGraph->add_resource_triple('http://talisaspire.com/foo/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://purl.org/ontology/bibo/Book');
//        $newGraph->add_literal_triple('http://talisaspire.com/foo/1', 'http://talisaspire.com/schema#foo1', 'foo2');
//
//        // Prevent writes to Mongo but check what should be send to Elastic Search
//        $mockTripod = $this->getMock('MongoTripod', array('generateSearchDocument', 'storeChanges'), array('CBD_testing','testing'));
//        $mockTripod->expects($this->any())->method('storeChanges')->will($this->returnValue(true));
//        $mockTripod->expects($this->any())->method('generateSearchDocument');
//        //$mockTripod->expects($this->once())->method('generateSearchDocument')->with('http://purl.org/ontology/bibo/Book', 'http://talisaspire.com/foo/1');
//
//        $mockTripod->saveChanges($oldGraph, $newGraph);
//    }

}
