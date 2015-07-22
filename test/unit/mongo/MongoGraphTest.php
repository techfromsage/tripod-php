<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoGraph.class.php';

/**
 * Class MongoGraphTest
 */
class MongoGraphTest extends MongoTripodTestBase
{
    protected function setUp()
    {
        parent::setup();
    }

    public function testUriToQNameOnRegisteredNS()
    {
        $g = new \Tripod\Mongo\MongoGraph();
        $this->assertEquals('dct:title',$g->uri_to_qname('http://purl.org/dc/terms/title'));
    }

    public function testUriToQNameOnUnRegisteredNS()
    {
        $this->setExpectedException('\Tripod\Exceptions\LabellerException', 'Could not label: http://someunregisteredns/');
        $g = new \Tripod\Mongo\MongoGraph();
        $g->uri_to_qname('http://someunregisteredns/title');
    }

    public function testQNameToUriOnUnRegisteredNS()
    {
        $this->setExpectedException('\Tripod\Exceptions\LabellerException', 'Could not label: someunregisteredns:title');
        $g = new \Tripod\Mongo\MongoGraph();
        $g->qname_to_uri('someunregisteredns:title');
    }

    public function testToNQuadsThrowsInvalidArgumentException() {
        $this->setExpectedException('InvalidArgumentException', 'You must specify the context when serializing to nquads');
        $g = new \Tripod\Mongo\MongoGraph();
        $g->to_nquads(null);
    }

    public function testToNQuads()
    {
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple("http://example.com/1", $g->qname_to_uri("dct:title"),"some literal title");
        $g->add_resource_triple("http://example.com/1", $g->qname_to_uri("dct:source"),"http://www.google.com");

        $expected = "<http://example.com/1> <http://purl.org/dc/terms/title> \"some literal title\" <http://talisaspire.com/> .
<http://example.com/1> <http://purl.org/dc/terms/source> <http://www.google.com> <http://talisaspire.com/> .\n";
        $this->assertEquals($expected, $g->to_nquads(\Tripod\Mongo\Config::getInstance()->getDefaultContextAlias()));
    }

    public function testToNQuadsTwoGraphsWithDifferentContext()
    {
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple("http://example.com/1", $g->qname_to_uri("dct:title"),"some literal title");
        $g->add_resource_triple("http://example.com/1", $g->qname_to_uri("dct:source"),"http://www.google.com");

        $expected = "<http://example.com/1> <http://purl.org/dc/terms/title> \"some literal title\" <http://talisaspire.com/> .
<http://example.com/1> <http://purl.org/dc/terms/source> <http://www.google.com> <http://talisaspire.com/> .\n";
        $this->assertEquals($expected, $g->to_nquads("http://talisaspire.com/"));

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple("http://example.com/2", $g->qname_to_uri("dct:title"),"some literal title");
        $g->add_resource_triple("http://example.com/2", $g->qname_to_uri("dct:source"),"http://www.google.com");

        $expected = "<http://example.com/2> <http://purl.org/dc/terms/title> \"some literal title\" <http://wibble.talisaspire.com/> .
<http://example.com/2> <http://purl.org/dc/terms/source> <http://www.google.com> <http://wibble.talisaspire.com/> .\n";
        $this->assertEquals($expected, $g->to_nquads("http://wibble.talisaspire.com/"));

    }


    public function testAddTripodArrayThrowsException()
    {
        $this->setExpectedException('Exception', 'Value passed to add_tripod_array is not of type array');
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_tripod_array(null);
    }

    public function testAddTripodArraySingleDoc()
    {
        $doc = array(
            "_id"=>array("r"=>"http://talisaspire.com/works/4d101f63c10a6-2", "c"=>"http://talisaspire.com/works/4d101f63c10a6-2"),
            "_version"=>0,
            "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
            "rdf:type"=>array(
                array("u"=>"http://purl.org/ontology/bibo/Book"),
                array("u"=>"http://talisaspire.com/schema#Work")
            ),
            "bibo:isbn13"=>array("l"=>"9211234567890")
        );

        $expected = new \Tripod\Mongo\MongoGraph();
        $expected->add_literal_triple("http://talisaspire.com/works/4d101f63c10a6-2", $expected->qname_to_uri("bibo:isbn13"),"9211234567890");
        $expected->add_resource_triple("http://talisaspire.com/works/4d101f63c10a6-2", $expected->qname_to_uri("dct:subject"),"http://talisaspire.com/disciplines/physics");
        $expected->add_resource_triple("http://talisaspire.com/works/4d101f63c10a6-2", $expected->qname_to_uri("rdf:type"),"http://purl.org/ontology/bibo/Book");
        $expected->add_resource_triple("http://talisaspire.com/works/4d101f63c10a6-2", $expected->qname_to_uri("rdf:type"),"http://talisaspire.com/schema#Work");

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_tripod_array($doc);

        $this->assertEquals($expected, $g);
    }

    /**
     * @dataProvider addTripodArrayContainingInvalidLiteralValues_Provider
     */
    public function testAddTripodArrayContainingInvalidLiteralValues($value)
    {
        $doc = array(
            "_id"=>array("r"=>"http://talisaspire.com/works/4d101f63c10a6-2", "c"=>"http://talisaspire.com/works/4d101f63c10a6-2"),
            "_version"=>0,
            "rdf:type"=>array(
                array("l"=>$value),
                array("l"=>"a Value"),
            ),
            "bibo:isbn13"=>array("l"=>"9211234567890"),
            "bibo:isbn10"=>array("l"=>$value)
        );

        $expected = new \Tripod\Mongo\MongoGraph();
        $expected->add_literal_triple("http://talisaspire.com/works/4d101f63c10a6-2", $expected->qname_to_uri("bibo:isbn13"),"9211234567890");
        $expected->add_literal_triple("http://talisaspire.com/works/4d101f63c10a6-2", $expected->qname_to_uri("rdf:type"),"a Value");

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_tripod_array($doc);

        $this->assertEquals($expected, $g);
    }
    public function addTripodArrayContainingInvalidLiteralValues_Provider(){
        return array(
            array(null),
            array(new stdClass()),
            array(function(){})
        );
    }

    /**
     * @dataProvider addTripodArrayContainingInvalidResourceValues_Provider
     */
    public function testAddTripodArrayContainingInvalidResourceValues($value)
    {
        $doc = array(
            "_id"=>array("r"=>"http://talisaspire.com/works/4d101f63c10a6-2", "c"=>"http://talisaspire.com/works/4d101f63c10a6-2"),
            "_version"=>0,
            "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
            "dct:publisher"=>array("u"=>$value),
            "rdf:type"=>array(
                array("u"=>$value),
                array("u"=>"http://talisaspire.com/schema#Work")
            ),
        );

        $expected = new \Tripod\Mongo\MongoGraph();
        $expected->add_resource_triple("http://talisaspire.com/works/4d101f63c10a6-2", $expected->qname_to_uri("dct:subject"),"http://talisaspire.com/disciplines/physics");
        $expected->add_resource_triple("http://talisaspire.com/works/4d101f63c10a6-2", $expected->qname_to_uri("rdf:type"),"http://talisaspire.com/schema#Work");

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_tripod_array($doc);

        $this->assertEquals($expected, $g);
    }
    public function addTripodArrayContainingInvalidResourceValues_Provider(){
        return array(
            array(1),
            array(1.2),
            array(true),
            array(array()),
            array(null),
            array(new stdClass()),
            array(function(){})
        );
    }

    public function testAddTripodArrayWhenAddingViews()
    {
        // view contains 4 subgraphs
        $view = json_decode(file_get_contents(dirname(__FILE__)."/data/view.json"), true);
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_tripod_array($view);

        // graph should contain 4 subgraphs
        $this->assertEquals(4, count($g->get_subjects()));

        // assert each subgraph
        $this->assertHasLiteralTriple($g, 'http://example.com/resources/1', $g->qname_to_uri("dct:date"), "2003");
        $this->assertHasLiteralTriple($g, 'http://example.com/resources/1', $g->qname_to_uri("dct:title"), "Material cultures: why some things matter");
        $this->assertHasLiteralTriple($g, 'http://example.com/resources/1', $g->qname_to_uri("bibo:isbn10"), "1857286863");
        $this->assertHasLiteralTriple($g, 'http://example.com/resources/1', $g->qname_to_uri("bibo:volume"), "Consumption and space");
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1', $g->qname_to_uri("dct:publisher"), "http://example.com/organisations/1");
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1', $g->qname_to_uri("dct:source"), "http://example.com/catalog/542114");
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1', $g->qname_to_uri("bibo:authorList"), "http://example.com/resources/1/authors");
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1', $g->qname_to_uri("rdf:type"), "http://purl.org/ontology/bibo/Book");

        $this->assertHasLiteralTriple($g, 'http://example.com/people/1', $g->qname_to_uri("foaf:name"), "Miller, Daniel");
        $this->assertHasResourceTriple($g, 'http://example.com/people/1', $g->qname_to_uri("rdf:type"), "http://xmlns.com/foaf/0.1/Person");

        $this->assertHasLiteralTriple($g, 'http://example.com/organisations/1', $g->qname_to_uri("foaf:name"), "Routledge");
        $this->assertHasResourceTriple($g, 'http://example.com/organisations/1', $g->qname_to_uri("rdf:type"), "http://xmlns.com/foaf/0.1/Organization");

        $this->assertHasResourceTriple($g, 'http://example.com/resources/1/authors', $g->qname_to_uri("rdf:_1"), "http://example.com/people/1");
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1/authors', $g->qname_to_uri("rdf:type"), "http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq");
    }

    public function testToTripodArray()
    {
        $expected = array(
            "_id"=>array("r"=>"http://talisaspire.com/works/4d101f63c10a6-2", "c"=>"http://example.com/"),
            "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
            "rdf:type"=>array(
                array("u"=>"bibo:Book"),
                array("u"=>"acorn:Work")
            ),
            "bibo:isbn13"=>array("l"=>"9211234567890")
        );

        // create a graph adding properties to it
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple("http://talisaspire.com/works/4d101f63c10a6-2", $g->qname_to_uri("bibo:isbn13"),"9211234567890");
        $g->add_resource_triple("http://talisaspire.com/works/4d101f63c10a6-2", $g->qname_to_uri("dct:subject"),"http://talisaspire.com/disciplines/physics");
        $g->add_resource_triple("http://talisaspire.com/works/4d101f63c10a6-2", $g->qname_to_uri("rdf:type"),"http://purl.org/ontology/bibo/Book");
        $g->add_resource_triple("http://talisaspire.com/works/4d101f63c10a6-2", $g->qname_to_uri("rdf:type"),"http://talisaspire.com/schema#Work");

        $actual = $g->to_tripod_array("http://talisaspire.com/works/4d101f63c10a6-2", "http://example.com/");

        $this->assertEquals($expected, $actual);
    }

    public function testToTripodArrayReturnsNullIfDocNotInGraph()
    {
        $g = new \Tripod\Mongo\MongoGraph();
        $doc = $g->to_tripod_array("http://example.com/1", "http://example.com/");
        $this->assertNull($doc);
    }

    public function testToTripodViewArray()
    {
        $expected = array(
            "_id"=>array("r"=>"http://example.com/things/1", "c"=>"http://example.com/"),
            "value"=>array(
                "_impactIndex"=>array("http://example.com/things/1","http://example.com/things/2"),
                "_graphs"=>array(
                    array(
                        "_id"=>array("r"=>"http://example.com/things/1", "c"=>"http://example.com/"),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                        "rdf:type"=>array("u"=>"bibo:Book"),
                        "bibo:isbn13"=>array("l"=>"9211234567890")
                    ),
                    array(
                        "_id"=>array("r"=>"http://example.com/things/2", "c"=>"http://example.com/"),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/maths"),
                        "rdf:type"=>array("u"=>"acorn:Work")
                    )
                )
            ),
        );

        // create a graph adding properties to it
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_resource_triple("http://example.com/things/1", $g->qname_to_uri("dct:subject"),"http://talisaspire.com/disciplines/physics");
        $g->add_resource_triple("http://example.com/things/1", $g->qname_to_uri("rdf:type"),"http://purl.org/ontology/bibo/Book");
        $g->add_literal_triple( "http://example.com/things/1", $g->qname_to_uri("bibo:isbn13"),"9211234567890");
        $g->add_resource_triple("http://example.com/things/2", $g->qname_to_uri("dct:subject"),"http://talisaspire.com/disciplines/maths");
        $g->add_resource_triple("http://example.com/things/2", $g->qname_to_uri("rdf:type"),"http://talisaspire.com/schema#Work");

        $actual = $g->to_tripod_view_array("http://example.com/things/1", "http://example.com/");
        $this->assertEquals($expected, $actual);
    }

    public function testWriteLockedDocDoesNotExposeVersionOrLockPropertyInGraph()
    {
        $doc = array(
            "_id"=>array("r"=>"http://example.com/things/1", "c"=>"http://example.com/"),
            _VERSION=>0,
            _LOCKED_FOR_TRANS=>"transaction_234"
        );

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_tripod_array($doc);
        $this->assertTrue(count($g->get_index())==0,"Graph should contain no data");
    }
}