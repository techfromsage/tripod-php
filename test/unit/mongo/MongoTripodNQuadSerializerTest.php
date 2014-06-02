<?php
require_once dirname(__FILE__).'/../TripodTestBase.php';

class MongoTripodNQuadSerializerTest extends TripodTestBase
{
    protected function setUp()
    {
        parent::setup();
    }

    public function testSerializerSimple()
    {
        $g = new MongoGraph();
        $g->add_literal_triple("http://example.com/1", $g->qname_to_uri("dct:title"),"some literal title");
        $g->add_resource_triple("http://example.com/1", $g->qname_to_uri("dct:source"),"http://www.google.com");

        $expected = "<http://example.com/1> <http://purl.org/dc/terms/title> \"some literal title\" <http://talisaspire.com/> .
<http://example.com/1> <http://purl.org/dc/terms/source> <http://www.google.com> <http://talisaspire.com/> .\n";

        $serializer = new MongoTripodNQuadSerializer();
        $actual = $serializer->getSerializedIndex($g->_index, MongoTripodConfig::getInstance()->getDefaultContextAlias());

        $this->assertEquals($expected, $actual);
    }

    public function testSerializerWithMultipleSubjects()
    {
        $g = new MongoGraph();
        $docs = json_decode(file_get_contents(dirname(__FILE__).'/../data/resources.json'), true);
        foreach ($docs as $d)
        {
            $g->add_tripod_array($d);
        }

        $expected ="<http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> <http://purl.org/dc/terms/title> \"Title of resource 2\" <http://talisaspire.com/> .
<http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> <http://purl.org/dc/terms/title> \"Title of resource 1\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/ontology/bibo/isbn13> \"9780393929690\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#bookmarkReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/bookmarks> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#foo> \"wibble\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f300> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f340> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#listReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/lists> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#openLibraryUri> <http://openlibrary.org/books/OL10157958M> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#preferredMetadata> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/metadata> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#author> \"Ohanian\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#discipline> \"physics\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#isbn> \"9780393929690\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Engineering: general\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"PHYSICS\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Science\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> \"Physics 3rd Edition\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> \"Physics for Engineers and Scientists\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"engineering: general\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"physics\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> \"science\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#usedAt> \"0071\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Resource> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/2002/07/owl#sameAs> <http://talisaspire.com/isbn/9780393929690> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://purl.org/ontology/bibo/isbn13> \"9780393929691\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/schema#bookmarkReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/bookmarks> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/schema#foo> \"wibble\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f300> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f340> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/schema#listReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/lists> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/schema#openLibraryUri> <http://openlibrary.org/books/OL10157958M> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/schema#preferredMetadata> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/metadata> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#author> \"Ohanian\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#discipline> \"physics\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#isbn> \"9780393929690\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Engineering: general\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"PHYSICS\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Science\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#title> \"Physics 3rd Edition\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#title> \"Physics for Engineers and Scientists\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#topic> \"engineering: general\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#topic> \"physics\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#topic> \"science\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://talisaspire.com/searchTerms/schema#usedAt> \"0071\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Resource> <http://talisaspire.com/> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2> <http://www.w3.org/2002/07/owl#sameAs> <http://talisaspire.com/isbn/9780393929690> <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6> <http://purl.org/ontology/bibo/isbn13> \"9780393929691\" <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6> <http://purl.org/ontology/bibo/isbn13> \"9780393929691-2\" <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/searchTerms/schema#discipline> \"physics\" <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/schema#seeAlso> <http://talisaspire.com/works/4d101f63c10a6-2> <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Work> <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6-2> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6-2> <http://purl.org/ontology/bibo/isbn13> \"9780393929691\" <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6-2> <http://talisaspire.com/searchTerms/schema#discipline> \"physics\" <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6-2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> <http://talisaspire.com/> .
<http://talisaspire.com/works/4d101f63c10a6-2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Work> <http://talisaspire.com/> .
<http://talisaspire.com/works/lockedDoc> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> <http://talisaspire.com/> .
<http://talisaspire.com/works/lockedDoc> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Work> <http://talisaspire.com/> .
<http://basedata.com/b/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Work> <http://talisaspire.com/> .
<http://basedata.com/b/1> <http://talisaspire.com/schema#seeAlso> <http://talisaspire.com/works/4d101f63c10a6-2> <http://talisaspire.com/> .
<http://basedata.com/b/2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Work> <http://talisaspire.com/> .
<http://basedata.com/b/2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Work2> <http://talisaspire.com/> .
<http://basedata.com/b/2> <http://talisaspire.com/schema#seeAlso> <http://basedata.com/b/1> <http://talisaspire.com/> .
";
        
        $serializer = new MongoTripodNQuadSerializer();
        $actual = $serializer->getSerializedIndex($g->_index, MongoTripodConfig::getInstance()->getDefaultContextAlias());

        $this->assertEquals($expected, $actual);
    }
}