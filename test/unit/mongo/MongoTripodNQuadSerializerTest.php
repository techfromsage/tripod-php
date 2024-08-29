<?php

use \Tripod\Mongo\MongoGraph;
use \Tripod\Mongo\NQuadSerializer;

class MongoTripodNQuadSerializerTest extends MongoTripodTestBase
{
    protected function setUp(): void
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

        $serializer = new NQuadSerializer();
        $actual = $serializer->getSerializedIndex($g->_index, \Tripod\Config::getInstance()->getDefaultContextAlias());

        $this->assertEquals($expected, $actual);
    }

    public function testSerializerWithMultipleSubjects()
    {
        $g = new MongoGraph();
        $docs = json_decode(file_get_contents(dirname(__FILE__).'/data/resources.json'), true);
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
<http://talisaspire.com/resources/indexKeyTooLarge> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://purl.org/ontology/bibo/isbn13> \"1234567890123\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/schema#bookmarkReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/bookmarks> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/schema#foo> \"wibble\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f300> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f340> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/schema#listReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/lists> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/schema#openLibraryUri> <http://openlibrary.org/books/OL10157958M> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/schema#preferredMetadata> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/metadata> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#author> \"Ohanian\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#discipline> \"physics\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#isbn> \"1234567890\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Engineering: general\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"PHYSICS\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> \"Science\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#title> \"Mahommah Gardo Baquaqua. Biography of Mahommah G. Baquaqua, a Native of Zoogoo, in the Interior of Africa. (A Convert to Christianity,) With a Description of That Part of the World; Including the Manners and Customs of the Inhabitants, Their Religious Notions, Form of Government, Laws, Appearance of the Country, Buildings, Agriculture, Manufactures, Shepherds and Herdsmen, Domestic Animals, Marriage Ceremonials, Funeral Services, Styles of Dress, Trade and Commerce, Modes of Warfare, System of Slavery, &amp;c., &amp;c. Mahommah&#039;s Early Life, His Education, His Capture and Slavery in Western Africa and Brazil, His Escape to the United States, from Thence to Hayti, (the City of Port Au Prince,) His Reception by the Baptist Missionary There, The Rev. W. L. Judd; His Conversion to Christianity, Baptism, and Return to This Country, His Views, Objects and Aim. Written and Revised from His Own Words, by Samuel Moore, Esq., Late Publisher of the &quot;North of England Shipping Gazette,&quot; Author of Several Popular Works, and Editor of Sundry Reform Papers.\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#topic> \"engineering: general\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#topic> \"physics\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#topic> \"science\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://talisaspire.com/searchTerms/schema#usedAt> \"0071\" <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#ResourceForTruncating> <http://talisaspire.com/> .
<http://talisaspire.com/resources/indexKeyTooLarge> <http://www.w3.org/2002/07/owl#sameAs> <http://talisaspire.com/isbn/9780393929690> <http://talisaspire.com/> .
<http://jacs3.dataincubator.org/f300> <http://purl.org/dc/terms/title> \"First title\" <http://talisaspire.com/> .
<http://jacs3.dataincubator.org/f300> <http://purl.org/dc/terms/title> \"Second title\" <http://talisaspire.com/> .
<http://jacs3.dataincubator.org/f340> <http://purl.org/dc/terms/title> \"First title\" <http://talisaspire.com/> .
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
<tenantUsers:s2123> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdfs.org/sioc/spec/User> <http://talisaspire.com/> .
<tenantUsers:s2123> <http://xmlns.com/foaf/0.1/firstName> \"Harry\" <http://talisaspire.com/> .
<tenantUsers:s2123> <http://xmlns.com/foaf/0.1/surname> \"Potter\" <http://talisaspire.com/> .
<tenantUsers:s2123> <http://lists.talis.com/schema/temp#last_login> \"2014-03-25T15:07:12+00:00\" <http://talisaspire.com/> .
<tenantUsers:s2123> <http://lists.talis.com/schema/temp#last_login_invalid> \"invalid\" <http://talisaspire.com/> .
<http://basedata.com/b/foo1234> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Document> <http://talisaspire.com/> .
<http://basedata.com/b/foo1234> <http://purl.org/dc/terms/creator> <http://schemas.talis.com/2005/user/schema#10101> <http://talisaspire.com/> .
<http://basedata.com/b/foo1234> <http://purl.org/dc/terms/title> \"A document title\" <http://talisaspire.com/> .
<http://basedata.com/b/foo1234> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/> .
<http://schemas.talis.com/2005/user/schema#10101> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://talisaspire.com/> .
<http://schemas.talis.com/2005/user/schema#10101> <http://xmlns.com/foaf/0.1/name> \"A. Person\" <http://talisaspire.com/> .
<http://schemas.talis.com/2005/user/schema#10101> <http://xmlns.com/foaf/0.1/knows> <http://schemas.talis.com/2005/user/schema#10102> <http://talisaspire.com/> .
<http://schemas.talis.com/2005/user/schema#10102> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://talisaspire.com/> .
<http://schemas.talis.com/2005/user/schema#10102> <http://xmlns.com/foaf/0.1/name> \"Anne O'ther\" <http://talisaspire.com/> .
<http://basedata.com/b/bar1234> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Document> <http://talisaspire.com/> .
<http://basedata.com/b/bar1234> <http://purl.org/dc/terms/creator> <http://schemas.talis.com/2005/user/schema#10103> <http://talisaspire.com/> .
<http://basedata.com/b/bar1234> <http://purl.org/dc/terms/contributor> <http://schemas.talis.com/2005/user/schema#10101> <http://talisaspire.com/> .
<http://basedata.com/b/bar1234> <http://purl.org/dc/terms/contributor> <http://schemas.talis.com/2005/user/schema#10102> <http://talisaspire.com/> .
<http://basedata.com/b/bar1234> <http://purl.org/dc/terms/title> \"Another document title\" <http://talisaspire.com/> .
<http://basedata.com/b/docWithEmptySeq123> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://basedata.com/b/DocWithSequence> <http://talisaspire.com/> .
<http://basedata.com/b/docWithEmptySeq123> <http://basedata.com/b/hasSequence> <http://basedata.com/b/sequence123> <http://talisaspire.com/> .
<http://basedata.com/b/docWithEmptySeq123> <http://purl.org/dc/terms/creator> <http://schemas.talis.com/2005/user/schema#xyz> <http://talisaspire.com/> .
<http://basedata.com/b/docWithEmptySeq123> <http://purl.org/dc/terms/title> \"Doc with sequence\" <http://talisaspire.com/> .";

        $serializer = new NQuadSerializer();
        $actual = $serializer->getSerializedIndex($g->_index, \Tripod\Config::getInstance()->getDefaultContextAlias());

        // This test initially asserted that $expected was equal to $actual
        //   $this->assertEquals($expected, $actual);
        // However, each time test data was added - it would also need to be added to expected above.
        // Rather than increasing the size of $expected further when adding new test data
        // this test now asserts that each line in $expected has been serialised correctly, without failing
        // due to new test data.
        foreach(preg_split("/((\r?\n)|(\r\n?))/", $expected) as $expectedLine){
            $this->assertTrue(strpos($actual, rtrim($expectedLine)) !== false, "Failed checking for line: " . rtrim($expectedLine));
        }


    }
}
