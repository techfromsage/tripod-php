<?php

declare(strict_types=1);

use MongoDB\BSON\Binary;
use MongoDB\BSON\ObjectId;
use MongoDB\BSON\Regex;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Driver\ReadPreference;
use MongoDB\InsertOneResult;
use MongoDB\UpdateResult;
use Tripod\ChangeSet;
use Tripod\Config;
use Tripod\Exceptions\CardinalityException;
use Tripod\Exceptions\ConfigException;
use Tripod\ExtendedGraph;
use Tripod\IEventHook;
use Tripod\Mongo\Composites\Tables;
use Tripod\Mongo\Composites\Views;
use Tripod\Mongo\DateUtil;
use Tripod\Mongo\Driver;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Jobs\DiscoverImpactedSubjects;
use Tripod\Mongo\Labeller;
use Tripod\Mongo\MongoGraph;
use Tripod\Mongo\TransactionLog;
use Tripod\Mongo\Updates;
use Tripod\StatsD;

class MongoTripodDriverTest extends MongoTripodTestBase
{
    protected function setUp(): void
    {
        parent::setup();

        $this->tripodTransactionLog = new TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing', [
            'defaultContext' => 'http://talisaspire.com/',
        ]);

        $this->getTripodCollection($this->tripod)->drop();

        // Lock collection no longer available from Tripod, so drop it manually
        Config::getInstance()->getCollectionForLocks($this->tripod->getStoreName())->drop();

        $this->tripod->setTransactionLog($this->tripodTransactionLog);
    }

    public function testSelectMultiValue(): void
    {
        $this->loadResourceDataViaTripod();

        $expectedResult = [
            'head' => [
                'count' => 1,
                'offset' => 0,
                'limit' => null,
            ],
            'results' => [
                [
                    '_id' => [
                        _ID_RESOURCE => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',
                        _ID_CONTEXT => 'http://talisaspire.com/',
                    ],
                    'dct:source' => [
                        'http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53',
                        'http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9',
                    ],
                ],
            ],
        ];
        $actualResult = $this->tripod->select(['bibo:isbn13.' . VALUE_LITERAL => '9780393929690'], ['dct:source' => true]);
        $this->assertEquals($expectedResult, $actualResult);
    }

    public function testSelectSingleValue(): void
    {
        $this->loadResourceDataViaTripod();

        $expectedResult = [
            'head' => [
                'count' => 1,
                'offset' => 0,
                'limit' => null,
            ],
            'results' => [
                [
                    '_id' => [
                        _ID_RESOURCE => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',
                        _ID_CONTEXT => 'http://talisaspire.com/',
                    ],
                    'dct:subject' => 'http://talisaspire.com/disciplines/physics',
                ],
            ],
        ];
        $actualResult = $this->tripod->select(['bibo:isbn13.' . VALUE_LITERAL => '9780393929690'], ['dct:subject' => true]);
        $this->assertEquals($expectedResult, $actualResult);
    }

    public function testGraph(): void
    {
        $this->loadResourceDataViaTripod();

        $expectedResult = new ExtendedGraph();
        $expectedResult->add_turtle(
            '<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/ontology/bibo/isbn13> "9780393929690" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#bookmarkReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/bookmarks> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#foo> "wibble" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f300> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f340> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#listReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/lists> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#openLibraryUri> <http://openlibrary.org/books/OL10157958M> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#preferredMetadata> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/metadata> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#author> "Ohanian" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#discipline> "physics" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#isbn> "9780393929690" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> "Engineering: general" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> "PHYSICS" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> "Science" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> "Physics 3rd Edition" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> "Physics for Engineers and Scientists" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> "engineering: general" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> "physics" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> "science" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#usedAt> "0071" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Resource> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "Testing" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/2002/07/owl#sameAs> <http://talisaspire.com/isbn/9780393929690> .
'
        );
        $actualResult = $this->tripod->graph(['bibo:isbn13.' . VALUE_LITERAL => '9780393929690']);

        $cs = new ChangeSet(['before' => $expectedResult->get_index(), 'after' => $actualResult->get_index(), 'changeReason' => 'testing!']);

        $this->assertFalse($cs->has_changes());
    }

    public function testDescribeResource(): void
    {
        $this->loadResourceDataViaTripod();

        $expectedResult = new ExtendedGraph();
        $expectedResult->add_turtle(
            '<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/ontology/bibo/isbn13> "9780393929690" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#bookmarkReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/bookmarks> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#foo> "wibble" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f300> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f340> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#listReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/lists> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#openLibraryUri> <http://openlibrary.org/books/OL10157958M> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#preferredMetadata> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/metadata> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#author> "Ohanian" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#discipline> "physics" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#isbn> "9780393929690" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> "Engineering: general" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> "PHYSICS" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> "Science" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> "Physics 3rd Edition" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> "Physics for Engineers and Scientists" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> "engineering: general" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> "physics" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> "science" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#usedAt> "0071" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Resource> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "Testing" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/2002/07/owl#sameAs> <http://talisaspire.com/isbn/9780393929690> .
'
        );
        $actualResult = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');

        $cs = new ChangeSet(['before' => $expectedResult->get_index(), 'after' => $actualResult->get_index(), 'changeReason' => 'testing!']);

        $this->assertFalse($cs->has_changes());
    }

    public function testDescribeResources(): void
    {
        $this->loadResourceDataViaTripod();

        $expectedResult = new ExtendedGraph();
        $expectedResult->add_turtle(
            '<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/isVersionOf> <http://talisaspire.com/works/4d101f63c10a6> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/BFBC6A06-A8B0-DED8-53AA-8E80DB44CC53> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/source> <http://life.ac.uk/resources/836E7CAD-63D2-63A0-B1CB-AA6A7E54A5C9> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://purl.org/ontology/bibo/isbn13> "9780393929690" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#bookmarkReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/bookmarks> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#foo> "wibble" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f300> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#jacsUri> <http://jacs3.dataincubator.org/f340> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#listReferences> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/lists> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#openLibraryUri> <http://openlibrary.org/books/OL10157958M> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/schema#preferredMetadata> <http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA/metadata> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#author> "Ohanian" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#discipline> "physics" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#isbn> "9780393929690" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> "Engineering: general" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> "PHYSICS" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#openLibrarySubject> "Science" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> "Physics 3rd Edition" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#title> "Physics for Engineers and Scientists" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> "engineering: general" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> "physics" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#topic> "science" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://talisaspire.com/searchTerms/schema#usedAt> "0071" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Resource> .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "Testing" .
<http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA> <http://www.w3.org/2002/07/owl#sameAs> <http://talisaspire.com/isbn/9780393929690> .
<http://talisaspire.com/works/4d101f63c10a6> <http://purl.org/dc/terms/subject> <http://talisaspire.com/disciplines/physics> .
<http://talisaspire.com/works/4d101f63c10a6> <http://purl.org/ontology/bibo/isbn13> "9780393929691" .
<http://talisaspire.com/works/4d101f63c10a6> <http://purl.org/ontology/bibo/isbn13> "9780393929691-2" .
<http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/searchTerms/schema#discipline> "physics" .
<http://talisaspire.com/works/4d101f63c10a6> <http://talisaspire.com/schema#seeAlso> <http://talisaspire.com/works/4d101f63c10a6-2> .
<http://talisaspire.com/works/4d101f63c10a6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Book> .
<http://talisaspire.com/works/4d101f63c10a6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://talisaspire.com/schema#Work> .
'
        );
        $actualResult = $this->tripod->describeResources(['http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'http://talisaspire.com/works/4d101f63c10a6']);

        $cs = new ChangeSet(['before' => $expectedResult->get_index(), 'after' => $actualResult->get_index(), 'changeReason' => 'testing!']);

        $this->assertFalse($cs->has_changes());
    }

    public function testGetCount(): void
    {
        $this->loadResourceDataViaTripod();

        $count = $this->tripod->getCount(['rdf:type.' . VALUE_URI => 'bibo:Book']);
        $this->assertEquals(9, $count);
    }

    public function testGetCountWithGroupBy(): void
    {
        $this->loadResourceDataViaTripod();

        $count = $this->tripod->getCount(['rdf:type.' . VALUE_URI => 'bibo:Book'], 'bibo:isbn13.l');

        $this->assertCount(5, $count);
        $this->assertEquals(2, $count['1234567890123']);
        $this->assertEquals(1, $count['']);
        $this->assertEquals(1, $count['9780393929691;9780393929691-2']);
        $this->assertEquals(4, $count['9780393929691']);
        $this->assertEquals(1, $count['9780393929690']);
    }

    public function testTripodSaveChangesRemovesLiteralTriple(): void
    {
        $this->loadResourceDataViaTripod();

        $oG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $nG = new MongoGraph();
        $nG->add_graph($oG);
        $nG->remove_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition');

        $this->tripod->saveChanges($oG, $nG, 'http://talisaspire.com/', 'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $this->assertEquals($nG, $uG, 'Updated does not match expected graph');
    }

    public function testTripodSaveChangesAddsLiteralTriple(): void
    {
        $this->loadResourceDataViaTripod();

        $oG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $nG = new MongoGraph();
        $nG->add_graph($oG);
        $nG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges($oG, $nG, 'http://talisaspire.com/', 'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $this->assertEquals($nG, $uG, 'Updated does not match expected graph');
    }

    /**
     * this test verifies that if we know we want to remove a specific triple from a document
     * we dont have to load the whole document in as the old graph, we just enumerate the single triple we want removed
     * what should happen is that the cs builder will translate that into a single removal.
     */
    public function testTripodSaveChangesRemovesLiteralTripleUsingEmptyNewGraphAndPartialOldGraph(): void
    {
        $this->loadResourceDataViaTripod();

        $oG = new MongoGraph();
        $oG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $oG->qname_to_uri('bibo:isbn13'), '9780393929690');

        $nG = new MongoGraph();

        $this->tripod->saveChanges($oG, $nG, 'http://talisaspire.com/', 'my changes');

        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');

        $this->assertTrue($uG->has_triples_about('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA'));
        $this->assertDoesNotHaveLiteralTriple($uG, 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $oG->qname_to_uri('bibo:isbn13'), '9780393929690');
    }

    /**
     * this test verifies that if we simply want to add some data to a document that exists in we dont need to specify an oldgraph; we just need to specify the new graph
     * the cs builder should translate that into a single addition statement and apply it.
     */
    public function testTripodSaveChangesAddsLiteralTripleUsingEmptyOldGraph(): void
    {
        $this->loadResourceDataViaTripod();

        $oG = new MongoGraph();
        $nG = new MongoGraph();
        $nG->add_graph($oG);
        $nG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges($oG, $nG, 'http://talisaspire.com/', 'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $this->assertHasLiteralTriple($uG, 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');
    }

    public function testTripodSaveChangesUpdatesLiteralTriple(): void
    {
        $this->loadResourceDataViaTripod();

        $oG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        $nG = new MongoGraph();
        $nG->add_graph($oG);
        $nG->remove_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition');
        $nG->add_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');
        // echo $nG->to_rdfxml()."\n";

        $this->tripod->saveChanges($oG, $nG, 'http://talisaspire.com/', 'my changes');
        $uG = $this->tripod->describeResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');
        // echo $uG->to_rdfxml()."\n";

        $this->assertTrue($uG->has_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE'), 'Graph should contain literal triple we added');
        $this->assertFalse($uG->has_literal_triple('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $nG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition'), 'Graph should not contain literal triple we removed');
    }

    public function testSaveCompletelyNewGraph(): void
    {
        $uri = 'http://example.com/resources/1';

        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource'));
        $g->add_literal_triple($uri, $g->qname_to_uri('dct:title'), 'wibble');

        $this->tripod->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/', 'something new');

        $uG = $this->tripod->describeResource($uri);

        $this->assertTrue($uG->has_triples_about($uri), 'new entity we created was not saved');
        $this->assertTrue($uG->has_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')), 'Graph should contain resource triple we added');
        $this->assertTrue($uG->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'wibble'), 'Graph should contain literal triple we added');
    }

    public function testRemoveGraphEntirely(): void
    {
        $uri = 'http://example.com/resources/1';

        // create a new entity and save it
        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource'));
        $g->add_literal_triple($uri, $g->qname_to_uri('dct:title'), 'wibble');

        $this->tripod->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/', 'something new');

        // retrieve it and make sure it was saved correctly
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_triples_about($uri), 'new entity we created was not saved');
        $this->assertTrue($uG->has_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')), 'Graph should contain resource triple we added');
        $this->assertTrue($uG->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'wibble'), 'Graph should contain literal triple we added');

        // now remove all knowledge about it, then describe the resource again, should be an empty graph
        $this->tripod->saveChanges($uG, new MongoGraph(), 'http://talisaspire.com/', 'murder death kill');
        $g = $this->tripod->describeResource($uri);

        $this->assertTrue($g->is_empty());
    }

    public function testSaveFailsWhenOldGraphIsInvalidNoDataInStoreForObj(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Error storing changes');

        $uri = 'http://example.com/resources/1';

        // create a new entity and save it
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));

        $nG = new MongoGraph();
        $nG->add_resource_triple($uri, $nG->qname_to_uri('rdf:type'), $nG->qname_to_uri('acorn:List'));

        $this->tripod->saveChanges($oG, $nG, 'http://talisaspire.com/');
    }

    public function testInterleavingUpdateFailsIfUnderlyingDataHasChanged(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Error storing changes');

        $uri = 'http://example.com/resources/1';

        $g = new MongoGraph();
        // canned response will simulate that the underlying data has changed
        $doc = ['_id' => $uri, 'rdf:type' => [['value' => $g->qname_to_uri('acorn:Resource'), 'type' => 'uri']]];

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                ['defaultContext' => 'http://talisaspire.com/'],
            ])
            ->getMock();

        $mockTripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['getDocumentForUpdate'])
            ->setConstructorArgs([$mockTripod])
            ->getMock();

        $mockTripodUpdate->expects($this->once())
            ->method('getDocumentForUpdate')
            ->with($uri)
            ->willReturn($doc);

        $mockTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdate);
        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Book'));

        $nG = new MongoGraph();
        $nG->add_resource_triple($uri, $nG->qname_to_uri('rdf:type'), $nG->qname_to_uri('acorn:Foo'));

        $mockTripod->saveChanges($oG, $nG, 'http://talisaspire.com/');
    }

    public function testInterleavingUpdateFailsIfCriteriaIsNotValidAtPointOfSave(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Error storing changes');

        // save some data in the store
        $uri = 'http://example.com/resources/1';

        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Book'));

        $this->tripod->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/');

        // canned response will simulate that the underlying data has changed
        $doc = ['_id' => $uri, '_version' => 3, 'rdf:type' => [['value' => $g->qname_to_uri('acorn:Resource'), 'type' => 'uri']]];

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                ['defaultContext' => 'http://talisaspire.com/'],
            ])
            ->getMock();

        $mockTripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['getDocumentForUpdate'])
            ->setConstructorArgs([$mockTripod])
            ->getMock();

        $mockTripodUpdate->expects($this->once())
            ->method('getDocumentForUpdate')
            ->with($uri)
            ->willReturn($doc);

        $mockTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdate);

        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));

        $nG = new MongoGraph();
        $nG->add_resource_triple($uri, $nG->qname_to_uri('rdf:type'), $nG->qname_to_uri('acorn:Foo'));

        $mockTripod->saveChanges($oG, $nG, 'http://talisaspire.com/');
    }

    public function testAddMultipleTriplesForSameProperty(): void
    {
        $uri = 'http://example.com/resources/1';

        // save a graph to the store
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Some title');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Another title');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Yet another title');

        $this->tripod->saveChanges(new MongoGraph(), $oG, 'http://talisaspire.com/');

        // retrieve it and make sure it was saved correctly
        $g = $this->tripod->describeResource($uri);
        $this->assertTrue($g->has_triples_about($uri), 'new entity we created was not saved');
        $this->assertTrue($g->has_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')), 'Graph should contain resource triple we added');
        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Some title'), 'Graph should contain literal triple we added');
        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Another title'), 'Graph should contain literal triple we added');
        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Yet another title'), 'Graph should contain literal triple we added');
    }

    public function testRemoveMultipleTriplesForSameProperty(): void
    {
        $uri = 'http://example.com/resources/1';

        // save a graph to the store
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Some title');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Another title');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Yet another title');

        $this->tripod->saveChanges(new MongoGraph(), $oG, 'http://talisaspire.com/');

        // remove all three dct:title triples
        $g2 = new MongoGraph();
        $g2->add_resource_triple($uri, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));

        $this->tripod->saveChanges($oG, $g2, 'http://talisaspire.com/');

        $g = $this->tripod->describeResource($uri);
        $this->assertTrue($g->has_triples_about($uri), 'new entity we created was not saved');
        $this->assertTrue($g->has_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')), 'Graph should contain resource triple we added');
        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Some title'), 'Graph should not contain literal triple we removed');
        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Another title'), 'Graph should not contain literal triple we removed');
        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Yet another title'), 'Graph should not contain literal triple we removed');
    }

    public function testChangeMultipleTriplesForSamePropertySimple(): void
    {
        $uri = 'http://example.com/resources/1';

        // save a graph to the store
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Some title');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Another title');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Yet another title');

        $this->tripod->saveChanges(new MongoGraph(), $oG, 'http://talisaspire.com/');

        $g2 = new MongoGraph();
        $g2->add_resource_triple($uri, $g2->qname_to_uri('rdf:type'), $g2->qname_to_uri('acorn:Resource'));
        $g2->add_literal_triple($uri, $g2->qname_to_uri('dct:title'), 'Updated Some title');
        $g2->add_literal_triple($uri, $g2->qname_to_uri('dct:title'), 'Updated Another title');
        $g2->add_literal_triple($uri, $g2->qname_to_uri('dct:title'), 'Updated Yet another title');

        $this->tripod->saveChanges($oG, $g2, 'http://talisaspire.com/');

        $g = $this->tripod->describeResource($uri);

        $this->assertTrue($g->has_triples_about($uri), 'new entity we created was not saved');
        $this->assertTrue($g->has_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')), 'Graph should contain resource triple we added');

        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Some title'), 'Graph should not contain literal triple we removed');
        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Another title'), 'Graph should not contain literal triple we removed');
        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Yet another title'), 'Graph should not contain literal triple we removed');

        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Updated Some title'), 'Graph should  contain literal triple we added');
        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Updated Another title'), 'Graph should contain literal triple we added');
        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Updated Yet another title'), 'Graph should contain literal triple we added');
    }

    public function testChangeMultipleTriplesForSamePropertyMoreComplex(): void
    {
        $uri = 'http://example.com/resources/1';

        // save a graph to the store
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Title one');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Title two');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Title three');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Title four');
        $oG->add_literal_triple($uri, $oG->qname_to_uri('dct:title'), 'Title five');

        $this->tripod->saveChanges(new MongoGraph(), $oG, 'http://talisaspire.com/');

        // new data
        $g2 = new MongoGraph();
        $g2->add_resource_triple($uri, $g2->qname_to_uri('rdf:type'), $g2->qname_to_uri('acorn:Resource'));
        $g2->add_literal_triple($uri, $g2->qname_to_uri('dct:title'), 'New Title one');
        $g2->add_literal_triple($uri, $g2->qname_to_uri('dct:title'), 'New Title two');
        $g2->add_literal_triple($uri, $g2->qname_to_uri('dct:title'), 'Title five');
        $g2->add_literal_triple($uri, $g2->qname_to_uri('dct:title'), 'New Title seven');

        $this->tripod->saveChanges($oG, $g2, 'http://talisaspire.com/');

        $g = $this->tripod->describeResource($uri);

        $this->assertTrue($g->has_triples_about($uri), 'new entity we created was not saved');
        $this->assertTrue($g->has_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')), 'Graph should contain resource triple we added');

        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Title one'), 'Graph should not contain literal triple we removed');
        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Title two'), 'Graph should not contain literal triple we removed');
        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Title three'), 'Graph should not contain literal triple we removed');
        $this->assertFalse($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Title four'), 'Graph should not contain literal triple we removed');

        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'New Title one'), 'Graph should  contain literal triple we added');
        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'New Title two'), 'Graph should contain literal triple we added');
        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'Title five'), 'Graph should contain literal triple we added');
        $this->assertTrue($g->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'New Title seven'), 'Graph should contain literal triple we added');
    }

    public function testSetReadPreferenceWhenSavingChanges(): void
    {
        $subjectOne = 'http://talisaspire.com/works/checkReadPreferencesWrite';

        $tripodMock = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                ['defaultContext' => 'http://talisaspire.com/'],
            ])
            ->getMock();

        $tripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['setReadPreferenceToPrimary', 'resetOriginalReadPreference'])
            ->setConstructorArgs([$tripodMock])
            ->getMock();

        $tripodUpdate
            ->expects($this->once())
            ->method('setReadPreferenceToPrimary');

        $tripodUpdate
            ->expects($this->once())
            ->method('resetOriginalReadPreference');

        $tripodMock
            ->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdate);

        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri('dct:title'), 'Title one');

        $tripodMock->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/');
    }

    public function testReadPreferencesAreRestoredWhenErrorSavingChanges(): void
    {
        $subjectOne = 'http://talisaspire.com/works/checkReadPreferencesAreRestoredOnError';
        $tripodMock = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                ['defaultContext' => 'http://talisaspire.com/'],
            ])
            ->getMock();

        $tripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['resetOriginalReadPreference', 'getContextAlias'])
            ->setConstructorArgs([$tripodMock])
            ->getMock();

        $tripodUpdate
            ->expects($this->once())
            ->method('getContextAlias')
            ->willThrowException(new Exception('A Test Exception'));

        $tripodUpdate
            ->expects($this->once())
            ->method('resetOriginalReadPreference');

        $tripodMock
            ->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdate);

        $this->expectException(Exception::class);

        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri('dct:title'), 'Title one');

        $tripodMock->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/');
    }

    public function testReadPreferencesOverMultipleSaves(): void
    {
        $subjectOne = 'http://talisaspire.com/works/checkReadPreferencesOverMultipleSaves';

        $tripodMock = $this->getMockBuilder(TestTripod::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                ['defaultContext' => 'http://talisaspire.com/', 'readPreference' => ReadPreference::SECONDARY_PREFERRED],
            ])
            ->getMock();

        $tripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['validateGraphCardinality'])
            ->setConstructorArgs([$tripodMock])
            ->getMock();

        $tripodUpdate
            ->expects($this->exactly(3))
            ->method('validateGraphCardinality')->willReturnOnConsecutiveCalls(null, $this->throwException(new Exception('readPreferenceOverMultipleSavesTestException')), null);

        $tripodMock
            ->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdate);

        $expectedCollectionReadPreference = $tripodMock->getCollectionReadPreference();
        $this->assertEquals(ReadPreference::SECONDARY_PREFERRED, $expectedCollectionReadPreference->getModeString());

        // Assert that a simple save results in read preferences being restored
        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri('dct:title'), 'Title one');

        $tripodMock->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/');
        $this->assertEquals($expectedCollectionReadPreference, $tripodMock->getCollectionReadPreference());

        // Assert a thrown exception still results in read preferences being restored
        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri('dct:title2'), 'Title two');

        $exceptionThrown = false;

        try {
            $tripodMock->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/');
        } catch (Exception $e) {
            $exceptionThrown = true;
            $this->assertSame('readPreferenceOverMultipleSavesTestException', $e->getMessage());
        }

        $this->assertTrue($exceptionThrown);
        $this->assertEquals($expectedCollectionReadPreference, $tripodMock->getCollectionReadPreference());

        // Assert that a new save after the exception still results in read preferences being restored
        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri('dct:title3'), 'Title three');

        $tripodMock->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/');
        $this->assertEquals($expectedCollectionReadPreference, $tripodMock->getCollectionReadPreference());
    }

    public function testSaveChangesToLockedDocument(): void
    {
        $subjectOne = 'http://talisaspire.com/works/lockedDoc';

        $this->lockDocument($subjectOne, 'transaction_101');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Error storing changes: Did not obtain locks on documents');

        $g = new MongoGraph();
        $g->add_literal_triple($subjectOne, $g->qname_to_uri('dct:title'), 'Title one');

        $this->tripod->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/');
    }

    public function testSaveChangesToMultipleSubjects(): void
    {
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';

        // save a graph to the store containng two completely new entities
        $oG = new MongoGraph();
        $oG->add_resource_triple($subjectOne, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        $oG->add_literal_triple($subjectOne, $oG->qname_to_uri('dct:title'), 'Title one');
        $oG->add_literal_triple($subjectOne, $oG->qname_to_uri('dct:title'), 'Title two');
        $oG->add_resource_triple($subjectTwo, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Book'));
        $oG->add_literal_triple($subjectTwo, $oG->qname_to_uri('dct:title'), 'Title three');
        $oG->add_literal_triple($subjectTwo, $oG->qname_to_uri('dct:title'), 'Title four');

        $this->tripod->saveChanges(new MongoGraph(), $oG, 'http://talisaspire.com/');

        // retrieve them both, assert they are as we expect
        $g = $this->tripod->describeResources([$subjectOne, $subjectTwo]);
        $this->assertTrue($g->has_triples_about($subjectOne));
        $this->assertTrue($g->has_resource_triple($subjectOne, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')));
        $this->assertTrue($g->has_literal_triple($subjectOne, $g->qname_to_uri('dct:title'), 'Title one'));
        $this->assertTrue($g->has_literal_triple($subjectOne, $g->qname_to_uri('dct:title'), 'Title two'));
        $this->assertTrue($g->has_triples_about($subjectTwo));
        $this->assertTrue($g->has_resource_triple($subjectTwo, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Book')));
        $this->assertTrue($g->has_literal_triple($subjectTwo, $g->qname_to_uri('dct:title'), 'Title three'));
        $this->assertTrue($g->has_literal_triple($subjectTwo, $g->qname_to_uri('dct:title'), 'Title four'));

        // now lets save some changes to both
        $nG = new MongoGraph();
        $nG->add_graph($g);
        $nG->remove_literal_triple($subjectOne, $g->qname_to_uri('dct:title'), 'Title one');
        $nG->add_literal_triple($subjectOne, $g->qname_to_uri('dct:title'), 'Updated Title one');
        $nG->add_literal_triple($subjectOne, $g->qname_to_uri('dct:author'), 'Joe Bloggs');
        $nG->remove_literal_triple($subjectTwo, $g->qname_to_uri('dct:title'), 'Title four');
        $nG->remove_resource_triple($subjectTwo, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Book'));
        $nG->add_literal_triple($subjectTwo, $g->qname_to_uri('dct:title'), 'Updated Title four');
        $nG->add_literal_triple($subjectTwo, $g->qname_to_uri('dct:author'), 'James Brown');

        $this->tripod->saveChanges($g, $nG, 'http://talisaspire.com/');

        $uG = $this->tripod->describeResources([$subjectOne, $subjectTwo]);
        $this->assertTrue($uG->has_triples_about($subjectOne));
        $this->assertTrue($uG->has_resource_triple($subjectOne, $uG->qname_to_uri('rdf:type'), $uG->qname_to_uri('acorn:Resource')));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri('dct:title'), 'Updated Title one'));
        $this->assertFalse($uG->has_literal_triple($subjectOne, $uG->qname_to_uri('dct:title'), 'Title one'));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri('dct:title'), 'Title two'));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri('dct:author'), 'Joe Bloggs'));

        $this->assertTrue($uG->has_triples_about($subjectTwo));
        $this->assertFalse($uG->has_resource_triple($subjectTwo, $uG->qname_to_uri('rdf:type'), $uG->qname_to_uri('acorn:Book')));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri('dct:title'), 'Title three'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri('dct:title'), 'Updated Title four'));
        $this->assertFalse($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri('dct:title'), 'Title four'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri('dct:author'), 'James Brown'));
    }

    public function testDocumentVersioning(): void
    {
        $uri = 'http://example.com/resources/1';

        // save a new entity, and retrieve it
        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource'));
        $g->add_literal_triple($uri, $g->qname_to_uri('dct:title'), 'wibble');

        $this->tripod->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/', 'something new');
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_triples_about($uri), 'new entity we created was not saved');
        $this->assertTrue($uG->has_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')), 'Graph should contain resource triple we added');
        $this->assertTrue($uG->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'wibble'), 'Graph should contain literal triple we added');
        $this->assertDocumentVersion(['r' => $uri, 'c' => 'http://talisaspire.com/'], 0);

        $nG = new MongoGraph();
        $nG->add_graph($g);
        $nG->add_literal_triple($uri, $g->qname_to_uri('dct:title'), 'another title');

        $this->tripod->saveChanges($g, $nG, 'http://talisaspire.com/');
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')), 'Graph should contain resource triple we added');
        $this->assertTrue($uG->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'wibble'), 'Graph should contain literal triple we added');
        $this->assertTrue($uG->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'another title'), 'Graph should contain literal triple we added');
        $this->assertDocumentVersion(['r' => $uri, 'c' => 'http://talisaspire.com/'], 1);

        $nG = new MongoGraph();
        // $nG->add_graph();
        $nG->add_literal_triple($uri, $g->qname_to_uri('dct:title'), 'only a title');

        $this->tripod->saveChanges($uG, $nG, 'http://talisaspire.com/');

        $uG = $this->tripod->describeResource($uri);
        $this->assertFalse($uG->has_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource')), 'Graph should not contain resource triple we removed');
        $this->assertFalse($uG->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'wibble'), 'Graph should not contain literal triple we removed');
        $this->assertFalse($uG->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'another title'), 'Graph should not contain literal triple we removed');
        $this->assertTrue($uG->has_literal_triple($uri, $g->qname_to_uri('dct:title'), 'only a title'), 'Graph should contain literal triple we added');
        $this->assertDocumentVersion(['r' => $uri, 'c' => 'http://talisaspire.com/'], 2);

        // remove it completely
        $this->tripod->saveChanges($nG, new MongoGraph(), 'http://talisaspire.com/');
        $this->assertDocumentHasBeenDeleted(['r' => $uri, 'c' => 'http://talisaspire.com/']);
    }

    public function testSaveChangesWithInvalidCardinality(): void
    {
        $this->expectException(CardinalityException::class);
        $this->expectExceptionMessage("Cardinality failed on http://foo/bar/1 for 'rdf:type' - should only have 1 value and has: http://foo/bar#Class1, http://foo/bar#Class2");

        $config = [];
        $config['namespaces'] = ['rdf' => 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'tlog'];
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'mongodb://localhost:27017/',
            ],
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://abc:xyz@localhost:27018',
            ],
        ];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [
                        'cardinality' => [
                            'rdf:type' => 1,
                        ],
                    ],
                ],
            ],
        ];
        $config['queue'] = ['database' => 'queue', 'collection' => 'q_queue', 'data_source' => 'db'];

        // Override the config defined in base test class as we need specific config here.
        Config::setConfig($config);

        $tripod = new Driver('CBD_testing', 'tripod_php_testing', ['defaultContext' => 'http://talisaspire.com/']);

        $oldGraph = new ExtendedGraph();
        $newGraph = new ExtendedGraph();
        $newGraph->add_resource_triple('http://foo/bar/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://foo/bar#Class1');
        $newGraph->add_resource_triple('http://foo/bar/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://foo/bar#Class2');

        $tripod->saveChanges($oldGraph, $newGraph, 'http://talisaspire.com/');
    }

    public function testDiscoverImpactedSubjectsAreDoneAllOperationsASync(): void
    {
        $uri_1 = 'http://example.com/1';
        $uri_2 = 'http://example.com/2';
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri_1, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        $oG->add_resource_triple($uri_2, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));

        $stat = $this->getMockStat('example.com', 1234, 'foo.bar');
        $statsConfig = $stat->getConfig();

        // just updates, all three operations async
        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods([
                'getDataUpdater',
                'getComposite',
            ])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => 'http://talisaspire.com/',
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                    'statsConfig' => $statsConfig,
                ],
            ])
            ->getMock();

        $mockTripod->setStat($stat);

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([
                'storeChanges',
                'getDiscoverImpactedSubjects',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockDiscoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['createJob'])
            ->getMock();

        $labeller = new Labeller();

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias('http://example.com/1') => ['rdf:type'],
            $labeller->uri_to_alias('http://example.com/2') => ['rdf:type'],
        ];

        $jobData = [
            'changes' => $subjectsAndPredicatesOfChange,
            'operations' => [OP_TABLES, OP_VIEWS, OP_SEARCH],
            'storeName' => 'tripod_php_testing',
            'podName' => 'CBD_testing',
            'contextAlias' => 'http://talisaspire.com/',
            'statsConfig' => $statsConfig,
        ];

        // getComposite() should only be called if there are synchronous operations
        $mockTripod->expects($this->never())
            ->method('getComposite');
        $mockTripodUpdates->expects($this->once())
            ->method('getDiscoverImpactedSubjects')
            ->willReturn($mockDiscoverImpactedSubjects);

        $mockTripodUpdates->expects($this->once())
            ->method('storeChanges')
            ->willReturn(['subjectsAndPredicatesOfChange' => $subjectsAndPredicatesOfChange, 'transaction_id' => 't1234']);

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $mockDiscoverImpactedSubjects->expects($this->once())
            ->method('createJob')
            ->with(
                $jobData,
                Tripod\Mongo\Config::getDiscoverQueueName()
            );

        $mockTripod->saveChanges(new ExtendedGraph(), $oG, 'http://talisaspire.com/');
    }

    public function testDiscoverImpactedSubjectsForDeletionsSyncOpsAreDoneAsyncJobSubmitted(): void
    {
        $uri_1 = 'http://example.com/1';
        $uri_2 = 'http://example.com/2';
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri_1, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        $oG->add_resource_triple($uri_2, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        //        // just deletes, search only
        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater', 'getComposite'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => 'http://talisaspire.com/',
                    'async' => [OP_TABLES => false, OP_VIEWS => false, OP_SEARCH => true],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([
                'storeChanges',
                'getDiscoverImpactedSubjects',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockViews = $this->getMockBuilder(Views::class)
            ->onlyMethods(['getImpactedSubjects', 'update'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                'http://talisaspire.com/',
            ])
            ->getMock();

        $mockTables = $this->getMockBuilder(Tables::class)
            ->onlyMethods(['getImpactedSubjects', 'update'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                'http://talisaspire.com/',
            ])
            ->getMock();

        $mockDiscoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['createJob'])
            ->getMock();

        $labeller = new Labeller();

        // The predicates should be empty arrays, since these were deletes
        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias('http://example.com/1') => [],
            $labeller->uri_to_alias('http://example.com/2') => [],
        ];

        $jobData = [
            'changes' => $subjectsAndPredicatesOfChange,
            'operations' => [OP_SEARCH],
            'storeName' => 'tripod_php_testing',
            'podName' => 'CBD_testing',
            'contextAlias' => 'http://talisaspire.com/',
            'statsConfig' => [],
        ];

        $impactedViewSubjects = [
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => 'http://example.com/1',
                            _ID_CONTEXT => 'http://talisaspire.com',
                        ],
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing',
                    ]
                )
                ->onlyMethods(['update'])
                ->getMock(),
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => 'http://example.com/2',
                            _ID_CONTEXT => 'http://talisaspire.com',
                        ],
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing',
                    ]
                )
                ->onlyMethods(['update'])
                ->getMock(),
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => 'http://example.com/9',
                            _ID_CONTEXT => 'http://talisaspire.com',
                        ],
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing',
                    ]
                )
                ->onlyMethods(['update'])
                ->getMock(),
        ];
        $mockViews->expects($this->once())
            ->method('getImpactedSubjects')
            ->willReturn($impactedViewSubjects);

        $impactedViewSubjects[0]->expects($this->once())->method('update');
        $impactedViewSubjects[1]->expects($this->once())->method('update');
        $impactedViewSubjects[2]->expects($this->once())->method('update');

        // This shouldn't be called because ImpactedSubject->update has been mocked and isn't doing anything
        $mockViews->expects($this->never())->method('update');

        $impactedTableSubjects = [
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => 'http://example.com/1',
                            _ID_CONTEXT => 'http://talisaspire.com',
                        ],
                        OP_TABLES,
                        'tripod_php_testing',
                        'CBD_testing',
                    ]
                )
                ->onlyMethods(['update'])
                ->getMock(),
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => 'http://example.com/2',
                            _ID_CONTEXT => 'http://talisaspire.com',
                        ],
                        OP_TABLES,
                        'tripod_php_testing',
                        'CBD_testing',
                    ]
                )
                ->onlyMethods(['update'])
                ->getMock(),
        ];

        $mockTables->expects($this->once())
            ->method('getImpactedSubjects')
            ->willReturn($impactedTableSubjects);

        $impactedTableSubjects[0]->expects($this->once())->method('update');
        $impactedTableSubjects[1]->expects($this->once())->method('update');

        $mockTables->expects($this->never())->method('update');

        $mockTripodUpdates->expects($this->once())
            ->method('getDiscoverImpactedSubjects')
            ->willReturn($mockDiscoverImpactedSubjects);

        $mockTripodUpdates->expects($this->once())
            ->method('storeChanges')
            ->willReturn(['subjectsAndPredicatesOfChange' => $subjectsAndPredicatesOfChange, 'transaction_id' => 't1234']);

        $mockTripod->expects($this->exactly(2))
            ->method('getComposite')
            ->willReturnMap([
                [OP_TABLES, $mockTables],
                [OP_VIEWS, $mockViews],
            ]);

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $mockDiscoverImpactedSubjects->expects($this->once())
            ->method('createJob')
            ->with(
                $jobData,
                Tripod\Mongo\Config::getDiscoverQueueName()
            );

        $mockTripod->saveChanges($oG, new ExtendedGraph(), 'http://talisaspire.com/');
    }

    public function testDiscoverImpactedSubjectsForDefaultOperationsSetting(): void
    {
        $uri_1 = 'http://example.com/1';
        $uri_2 = 'http://example.com/2';
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri_1, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        $oG->add_resource_triple($uri_2, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));

        // a delete and an update
        $nG = new MongoGraph();
        $nG->add_graph($oG);
        $nG->add_literal_triple($uri_1, $nG->qname_to_uri('searchterms:title'), 'wibble');
        $nG->remove_resource_triple($uri_2, $oG->qname_to_uri('rdf:type'), 'http://foo/bar#Class2');

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods([
                'getComposite',
                'getDataUpdater',
            ])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                ['defaultContext' => 'http://talisaspire.com/'],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([
                'storeChanges',
                'getDiscoverImpactedSubjects',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockViews = $this->getMockBuilder(Views::class)
            ->onlyMethods(['getImpactedSubjects', 'update'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                'http://talisaspire.com/',
            ])
            ->getMock();

        $mockDiscoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['createJob'])
            ->getMock();

        $impactedViewSubjects = [
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => $uri_1,
                            _ID_CONTEXT => 'http://talisaspire.com',
                        ],
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing',
                    ]
                )
                ->onlyMethods(['update'])
                ->getMock(),
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => $uri_2,
                            _ID_CONTEXT => 'http://talisaspire.com',
                        ],
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing',
                    ]
                )
                ->onlyMethods(['update'])
                ->getMock(),
        ];

        $labeller = new Labeller();

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri_1) => ['searchterms:title'],
            $labeller->uri_to_alias($uri_2) => ['rdf:type'],
        ];

        $jobData = [
            'changes' => $subjectsAndPredicatesOfChange,
            'operations' => [OP_TABLES, OP_SEARCH],
            'storeName' => 'tripod_php_testing',
            'podName' => 'CBD_testing',
            'contextAlias' => 'http://talisaspire.com/',
            'statsConfig' => [],
        ];

        // getComposite() should only be called if there are synchronous operations
        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->willReturn($mockViews);

        $mockTripodUpdates->expects($this->once())
            ->method('getDiscoverImpactedSubjects')
            ->willReturn($mockDiscoverImpactedSubjects);

        $mockTripodUpdates->expects($this->once())
            ->method('storeChanges')
            ->willReturn(['subjectsAndPredicatesOfChange' => $subjectsAndPredicatesOfChange, 'transaction_id' => 't1234']);

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $mockViews->expects($this->once())
            ->method('getImpactedSubjects')
            ->willReturn($impactedViewSubjects);

        $impactedViewSubjects[0]->expects($this->once())->method('update');
        $impactedViewSubjects[1]->expects($this->once())->method('update');

        // This shouldn't be called because ImpactedSubject->update has been mocked and isn't doing anything
        $mockViews->expects($this->never())->method('update');

        $mockDiscoverImpactedSubjects->expects($this->once())
            ->method('createJob')
            ->with(
                $jobData,
                Tripod\Mongo\Config::getDiscoverQueueName()
            );

        $mockTripod->saveChanges($oG, $nG, 'http://talisaspire.com/');
    }

    public function testSpecifyQueueForAsyncOperations(): void
    {
        $uri_1 = 'http://example.com/1';
        $uri_2 = 'http://example.com/2';
        $oG = new MongoGraph();
        $oG->add_resource_triple($uri_1, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));
        $oG->add_resource_triple($uri_2, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri('acorn:Resource'));

        // a delete and an update
        $nG = new MongoGraph();
        $nG->add_graph($oG);
        $nG->add_literal_triple($uri_1, $nG->qname_to_uri('searchterms:title'), 'wibble');
        $nG->remove_resource_triple($uri_2, $oG->qname_to_uri('rdf:type'), 'http://foo/bar#Class2');

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods([
                'getComposite',
                'getDataUpdater',
            ])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                ['defaultContext' => 'http://talisaspire.com/'],
            ])
            ->getMock();

        $queueName = 'TRIPOD_TESTING_QUEUE_' . uniqid();

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([
                'storeChanges',
                'getDiscoverImpactedSubjects',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                        'queue' => $queueName,
                    ],
                ],
            ])
            ->getMock();

        $mockViews = $this->getMockBuilder(Views::class)
            ->onlyMethods(['getImpactedSubjects', 'update'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                'http://talisaspire.com/',
            ])
            ->getMock();

        $mockDiscoverImpactedSubjects = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['createJob'])
            ->getMock();

        $impactedViewSubjects = [
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => $uri_1,
                            _ID_CONTEXT => 'http://talisaspire.com',
                        ],
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing',
                    ]
                )
                ->onlyMethods(['update'])
                ->getMock(),
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => $uri_2,
                            _ID_CONTEXT => 'http://talisaspire.com',
                        ],
                        OP_VIEWS,
                        'tripod_php_testing',
                        'CBD_testing',
                    ]
                )
                ->onlyMethods(['update'])
                ->getMock(),
        ];

        $labeller = new Labeller();

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri_1) => ['searchterms:title'],
            $labeller->uri_to_alias($uri_2) => ['rdf:type'],
        ];

        $jobData = [
            'changes' => $subjectsAndPredicatesOfChange,
            'operations' => [OP_TABLES, OP_SEARCH],
            'storeName' => 'tripod_php_testing',
            'podName' => 'CBD_testing',
            'contextAlias' => 'http://talisaspire.com/',
            'queue' => $queueName,
            'statsConfig' => [],
        ];

        // getComposite() should only be called if there are synchronous operations
        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->willReturn($mockViews);

        $mockTripodUpdates->expects($this->once())
            ->method('getDiscoverImpactedSubjects')
            ->willReturn($mockDiscoverImpactedSubjects);

        $mockTripodUpdates->expects($this->once())
            ->method('storeChanges')
            ->willReturn(['subjectsAndPredicatesOfChange' => $subjectsAndPredicatesOfChange, 'transaction_id' => 't1234']);

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $mockViews->expects($this->once())
            ->method('getImpactedSubjects')
            ->willReturn($impactedViewSubjects);

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

        $mockTripod->saveChanges($oG, $nG, 'http://talisaspire.com/');
    }

    public function testWriteToUnconfiguredCollectionThrowsException(): void
    {
        //        Exception: testing:SOME_COLLECTION is not referenced within config, so cannot be written to
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Collection name 'SOME_COLLECTION' not in configuration");

        $tripod = new Driver('SOME_COLLECTION', 'tripod_php_testing');
        $tripod->saveChanges(new ExtendedGraph(), new ExtendedGraph(), 'http://talisaspire.com/');
    }

    // NAMESPACE TESTS

    /**
     * this test verifies that if we simply want to add some data to a document that exists in we dont need to specify an oldgraph; we just need to specify the new graph
     * the cs builder should translate that into a single addition statement and apply it.
     * This builds on the previous test, by operating on data in mongo where _id.r and _id.c are namespaced.
     */
    public function testTripodSaveChangesAddsLiteralTripleUsingEmptyOldGraphWithNamespacableIDAndContext(): void
    {
        $oG = new MongoGraph();
        $nG = new MongoGraph();
        $nG->add_graph($oG);

        // resource and context are namespaced in base data this time around...
        $nG->add_literal_triple('http://basedata.com/b/1', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges($oG, $nG, 'http://basedata.com/b/DefaultGraph', 'my changes');
        $uG = $this->tripod->describeResource('http://basedata.com/b/1', 'http://basedata.com/b/DefaultGraph');
        $this->assertHasLiteralTriple($uG, 'http://basedata.com/b/1', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');
    }

    /**
     * this test verifies that if we simply want to add some data to a document that exists in we dont need to specify an oldgraph; we just need to specify the new graph
     * the cs builder should translate that into a single addition statement and apply it.
     * This builds on the previous test, by operating on data in mongo where _id.r and _id.c are namespaced AND passing context into the save method.
     */
    public function testTripodSaveChangesAddsLiteralTripleUsingEmptyOldGraphWithNamespacedContext(): void
    {
        $oG = new MongoGraph();
        $nG = new MongoGraph();
        $nG->add_graph($oG);

        // resource and context are namespaced in base data this time around...
        $nG->add_literal_triple('http://basedata.com/b/1', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges($oG, $nG, 'baseData:DefaultGraph', 'my changes');
        $uG = $this->tripod->describeResource('http://basedata.com/b/1', 'http://basedata.com/b/DefaultGraph');
        $this->assertHasLiteralTriple($uG, 'http://basedata.com/b/1', $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');
    }

    public function testDescribeResourceWithNamespace(): void
    {
        $this->loadResourceDataViaTripod();

        $noNsG = $this->tripod->describeResource('http://basedata.com/b/1', 'http://basedata.com/b/DefaultGraph');
        $nsResourceG = $this->tripod->describeResource('baseData:1', 'http://basedata.com/b/DefaultGraph');
        $nsContextG = $this->tripod->describeResource('http://basedata.com/b/1', 'baseData:DefaultGraph');
        $nsBothG = $this->tripod->describeResource('baseData:1', 'baseData:DefaultGraph');

        $nsResourceCs = new ChangeSet(['before' => $noNsG->get_index(), 'after' => $nsResourceG->get_index(), 'changeReason' => 'testing!']);
        $this->assertFalse($nsResourceCs->has_changes(), 'Non ns and nsResource not equal');

        $nsContextCS = new ChangeSet(['before' => $noNsG->get_index(), 'after' => $nsContextG->get_index(), 'changeReason' => 'testing!']);
        $this->assertFalse($nsContextCS->has_changes(), 'Non ns and nsContext not equal');

        $nsBothCS = new ChangeSet(['before' => $noNsG->get_index(), 'after' => $nsBothG->get_index(), 'changeReason' => 'testing!']);
        $this->assertFalse($nsBothCS->has_changes(), 'Non ns and nsBoth not equal');
    }

    public function testDescribeResourcesWithNamespace(): void
    {
        $this->loadResourceDataViaTripod();

        $noNsG = $this->tripod->describeResources(['http://basedata.com/b/1'], 'http://basedata.com/b/DefaultGraph');
        $nsResourceG = $this->tripod->describeResources(['baseData:1'], 'http://basedata.com/b/DefaultGraph');
        $nsContextG = $this->tripod->describeResources(['http://basedata.com/b/1'], 'baseData:DefaultGraph');
        $nsBothG = $this->tripod->describeResources(['baseData:1'], 'baseData:DefaultGraph');

        $nsResourceCs = new ChangeSet(['before' => $noNsG->get_index(), 'after' => $nsResourceG->get_index(), 'changeReason' => 'testing!']);
        $this->assertFalse($nsResourceCs->has_changes(), 'Non ns and nsResource not equal');

        $nsContextCS = new ChangeSet(['before' => $noNsG->get_index(), 'after' => $nsContextG->get_index(), 'changeReason' => 'testing!']);
        $this->assertFalse($nsContextCS->has_changes(), 'Non ns and nsContext not equal');

        $nsBothCS = new ChangeSet(['before' => $noNsG->get_index(), 'after' => $nsBothG->get_index(), 'changeReason' => 'testing!']);
        $this->assertFalse($nsBothCS->has_changes(), 'Non ns and nsBoth not equal');
    }

    public function testSelectSingleValueWithNamespaceContextQueryDoesntContainID(): void
    {
        $this->loadResourceDataViaTripod();

        $expectedResult = [
            'head' => [
                'count' => 2,
                'offset' => 0,
                'limit' => null,
            ],
            'results' => [
                [
                    '_id' => [
                        _ID_RESOURCE => 'baseData:1',
                        _ID_CONTEXT => 'baseData:DefaultGraph',
                    ],
                    'rdf:type' => 'acorn:Work',
                ],
                [
                    '_id' => [
                        _ID_RESOURCE => 'baseData:2',
                        _ID_CONTEXT => 'baseData:DefaultGraph',
                    ],
                    'rdf:type' => ['acorn:Work', 'acorn:Work2'],
                ],
            ],
        ];
        $actualResult = $this->tripod->select(['rdf:type.' . VALUE_URI => 'acorn:Work'], ['rdf:type' => true], null, null, 0, 'baseData:DefaultGraph');
        $this->assertEquals($expectedResult, $actualResult);
    }

    public function testSelectSingleValueWithNamespaceContextQueryDoesContainID(): void
    {
        $this->loadResourceDataViaTripod();

        $expectedResult = [
            'head' => [
                'count' => 1,
                'offset' => 0,
                'limit' => null,
            ],
            'results' => [
                [
                    '_id' => [
                        _ID_RESOURCE => 'baseData:1',
                        _ID_CONTEXT => 'baseData:DefaultGraph',
                    ],
                    'rdf:type' => 'acorn:Work',
                ],
            ],
        ];
        $actualResult = $this->tripod->select(
            ['_id' => [_ID_RESOURCE => 'baseData:1']],
            ['rdf:type' => true],
            null,
            null,
            0,
            'baseData:DefaultGraph'
        );
        $this->assertEquals($expectedResult, $actualResult);
    }

    public function testSelectWithOperandWithNamespaceContextQueryContainsID(): void
    {
        $this->loadResourceDataViaTripod();

        $expectedResult = [
            'head' => [
                'count' => 2,
                'offset' => 0,
                'limit' => null,
            ],
            'results' => [
                [
                    '_id' => [
                        _ID_RESOURCE => 'baseData:1',
                        _ID_CONTEXT => 'baseData:DefaultGraph',
                    ],
                    'rdf:type' => 'acorn:Work',
                ],
                [
                    '_id' => [
                        _ID_RESOURCE => 'baseData:2',
                        _ID_CONTEXT => 'baseData:DefaultGraph',
                    ],
                    'rdf:type' => ['acorn:Work', 'acorn:Work2'],
                ],
            ],
        ];
        $actualResult = $this->tripod->select(
            ['_id' => ['$in' => [[_ID_RESOURCE => 'baseData:1'], [_ID_RESOURCE => 'baseData:2']]]],
            ['rdf:type' => true],
            null,
            null,
            0,
            'baseData:DefaultGraph'
        );
        $this->assertEquals($expectedResult, $actualResult);
    }

    public function testSelectWithOperandWithNamespaceContextQueryDoesNotContainID(): void
    {
        $this->loadResourceDataViaTripod();

        $expectedResult = [
            'head' => [
                'count' => 2,
                'offset' => 0,
                'limit' => null,
            ],
            'results' => [
                [
                    '_id' => [
                        _ID_RESOURCE => 'baseData:1',
                        _ID_CONTEXT => 'baseData:DefaultGraph',
                    ],
                    'rdf:type' => 'acorn:Work',
                ],
                [
                    '_id' => [
                        _ID_RESOURCE => 'baseData:2',
                        _ID_CONTEXT => 'baseData:DefaultGraph',
                    ],
                    'rdf:type' => ['acorn:Work', 'acorn:Work2'],
                ],
            ],
        ];
        $actualResult = $this->tripod->select(
            ['rdf:type' => ['$in' => [[VALUE_URI => 'acorn:Work']]]],
            ['rdf:type' => true],
            null,
            null,
            0,
            'baseData:DefaultGraph'
        );
        $this->assertEquals($expectedResult, $actualResult);
    }

    public function testSelectDocumentWithSpecialFieldTypes(): void
    {
        $this->loadResourceDataViaTripod();

        $id = [
            'r' => 'http://talisaspire.com/resources/' . uniqid(),
            'c' => 'http://talisaspire.com/',
        ];

        $config = Config::getInstance();
        $collection = $config->getCollectionForCBD($this->tripod->getStoreName(), $this->tripod->getPodName());
        $collection->insertOne([
            '_id' => $id,
            '_version' => 42,
            'rdf:type' => [
                'u' => 'dctype:Text',
            ],
            'dct:created' => [
                'l' => '2023-11-30T13:30:00Z',
            ],
            'dct:title' => [
                'l' => 'Test title',
            ],
            // Timestamps
            '_cts' => new UTCDateTime(1701351000000),
            '_uts' => new UTCDateTime(1701351000000),
            // Special field types
            '_oid' => new ObjectId(),
            '_bin' => new Binary('foo', Binary::TYPE_OLD_BINARY),
            '_fun' => new Regex('foo', 'i'),
        ]);

        $expectedResult = [
            'head' => [
                'count' => 1,
                'offset' => 0,
                'limit' => 0,
            ],
            'results' => [
                [
                    '_id' => $id,
                    '_version' => 42,
                    'rdf:type' => 'dctype:Text',
                    'dct:created' => '2023-11-30T13:30:00Z',
                    'dct:title' => 'Test title',
                ],
            ],
        ];

        $actualResult = $this->tripod->select(['_id' => $id], []);

        $this->assertEquals($expectedResult, $actualResult);
    }

    /**
     * Return the distinct values of a table column.
     */
    public function testGetDistinctTableValues(): void
    {
        $table = 't_distinct';
        $collection = Config::getInstance()->getCollectionForTable('tripod_php_testing', $table);
        $collection->drop();

        $this->loadResourceDataViaTripod();

        // Get table rows
        $this->tripod->generateTableRows($table);
        $rows = $this->tripod->getTableRows($table, [], [], 0, 0);
        $this->assertEquals(11, $rows['head']['count']);
        $results = $this->tripod->getDistinctTableColumnValues($table, 'value.title');

        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(4, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertCount(4, $results['results']);
        $this->assertContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        // Supply a filter
        $results = $this->tripod->getDistinctTableColumnValues($table, 'value.title', ['value.type' => 'bibo:Document']);
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(2, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertCount(2, $results['results']);
        $this->assertNotContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        $results = $this->tripod->getDistinctTableColumnValues($table, 'value.type');
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(7, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertCount(7, $results['results']);
        $this->assertContains('acorn:Resource', $results['results']);
        $this->assertContains('acorn:Work', $results['results']);
        $this->assertContains('bibo:Book', $results['results']);
        $this->assertContains('bibo:Document', $results['results']);
    }

    /**
     * Return no results for tablespec that doesn't exist.
     */
    public function testDistinctOnTableSpecThatDoesNotExist(): void
    {
        $table = 't_nothing_to_see_here';

        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Table id 't_nothing_to_see_here' not in configuration");
        $this->tripod->getDistinctTableColumnValues($table, 'value.foo');
    }

    /**
     * Return no results for distinct on a fieldname that is not defined in tableSpec.
     */
    public function testDistinctOnFieldNameThatIsNotInTableSpec(): void
    {
        // Get table rows
        $table = 't_distinct';
        $this->tripod->generateTableRows($table);
        $results = $this->tripod->getDistinctTableColumnValues($table, 'value.foo');
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    /**
     * Return no results for filters that match no table rows.
     */
    public function testDistinctForFilterWithNoMatches(): void
    {
        $this->loadResourceDataViaTripod();

        // Get table rows
        $table = 't_distinct';
        $this->tripod->generateTableRows($table);
        $results = $this->tripod->getDistinctTableColumnValues($table, 'value.title', ['value.foo' => 'wibble']);
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    /**  START: getLockedDocuments tests */
    public function testGetLockedDocuments(): void
    {
        $this->loadResourceDataViaTripod();

        $subject = 'http://talisaspire.com/works/lockedDoc';
        $this->lockDocument($subject, 'transaction_100');

        $docs = $this->tripod->getLockedDocuments();
        $this->assertCount(1, $docs);
        $this->assertEquals($docs[0]['_id']['r'], $subject);
        $this->assertEquals('transaction_100', $docs[0][_LOCKED_FOR_TRANS]);
    }

    public function testGetLockedDocumentsWithFromDateOnly(): void
    {
        $this->loadResourceDataViaTripod();

        $subject = 'http://talisaspire.com/works/lockedDoc';
        $this->lockDocument($subject, 'transaction_100');

        $docs = $this->tripod->getLockedDocuments(date('y-m-d H:i:s', strtotime('+1 min')));
        $this->assertCount(0, $docs);

        $docs = $this->tripod->getLockedDocuments(date('y-m-d H:i:s', strtotime('-1 min')));
        $this->assertCount(1, $docs);
    }

    public function testGetLockedDocumentsWithTillDateOnly(): void
    {
        $this->loadResourceDataViaTripod();

        $subject = 'http://talisaspire.com/works/lockedDoc';
        $this->lockDocument($subject, 'transaction_100');

        $docs = $this->tripod->getLockedDocuments(null, date('y-m-d H:i:s', strtotime('+1 min')));
        $this->assertCount(1, $docs);

        $docs = $this->tripod->getLockedDocuments(null, date('y-m-d H:i:s', strtotime('-1 min')));
        $this->assertCount(0, $docs);
    }

    public function testGetLockedDocumentsWithDateRange(): void
    {
        $this->loadResourceDataViaTripod();

        $subject = 'http://talisaspire.com/works/lockedDoc';
        $this->lockDocument($subject, 'transaction_100');

        $docs = $this->tripod->getLockedDocuments(date('y-m-d H:i:s', strtotime('-1 min')), date('y-m-d H:i:s', strtotime('+1 min')));
        $this->assertCount(1, $docs);

        $docs = $this->tripod->getLockedDocuments(date('y-m-d H:i:s', strtotime('+1 min')), date('y-m-d H:i:s', strtotime('+2 min')));
        $this->assertCount(0, $docs);
    }

    /** END: getLockedDocuments tests */

    /**  START: removeInertLocks tests */
    public function testRemoveInertLocksNoLocksFound(): void
    {
        $this->assertFalse($this->tripod->removeInertLocks('transaction_100', 'Unit tests'));
    }

    public function testRemoveInertLocksNotAllLocksAreRemoved(): void
    {
        $subjectOne = 'http://talisaspire.com/works/lockedDoc';
        $subjectTwo = 'http://basedata.com/b/1';

        $this->lockDocument($subjectOne, 'transaction_500');
        $this->lockDocument($subjectTwo, 'transaction_200');

        $docs = $this->tripod->getLockedDocuments();
        $this->assertCount(2, $docs);

        $this->tripod->removeInertLocks('transaction_200', 'Unit tests');
        $docs = $this->tripod->getLockedDocuments();
        $this->assertCount(1, $docs);
    }

    public function testRemoveInertLocksCreateAuditEntryThrowsException(): void
    {
        $subject = 'http://basedata.com/b/1';
        $this->lockDocument($subject, 'transaction_400');

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Some unexpected error occurred.');

        $auditManualRollbackCollection = $this->getMockBuilder(Collection::class)
            ->onlyMethods(['insertOne'])
            ->disableOriginalConstructor()
            ->getMock();
        $auditManualRollbackCollection->expects($this->once())
            ->method('insertOne')
            ->willThrowException(new Exception('Some unexpected error occurred.'));

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing', ['defaultContext' => 'http://talisaspire.com/']])
            ->getMock();
        $tripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['getAuditManualRollbacksCollection'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $tripodUpdate
            ->expects($this->once())
            ->method('getAuditManualRollbacksCollection')
            ->willReturn($auditManualRollbackCollection);

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdate);

        $tripod->removeInertLocks('transaction_400', 'Unit tests');
    }

    public function testRemoveInertLocksUnlockAllDocumentsFailsVerifyErrorEntryInAuditLog(): void
    {
        $subject = 'http://basedata.com/b/1';
        $this->lockDocument($subject, 'transaction_400');

        $mongoDocumentId = new ObjectId();
        $mongoDate = DateUtil::getMongoDate();

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Some unexpected error occurred.');

        $auditManualRollbackCollection = $this->getMockBuilder(Collection::class)
            ->onlyMethods(['updateOne', 'insertOne'])
            ->disableOriginalConstructor()
            ->getMock();

        $mockInsert = $this->getMockBuilder(InsertOneResult::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['isAcknowledged'])
            ->getMock();
        $mockInsert
            ->expects($this->once())
            ->method('isAcknowledged')
            ->willReturn(true);

        $auditManualRollbackCollection->expects($this->once())
            ->method('insertOne')
            ->willReturn($mockInsert);

        $mockUpdate = $this->getMockBuilder(UpdateResult::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['isAcknowledged'])
            ->getMock();
        $mockUpdate
            ->expects($this->once())
            ->method('isAcknowledged')
            ->willReturn(true);

        $auditManualRollbackCollection->expects($this->once())
            ->method('updateOne')
            ->with(['_id' => $mongoDocumentId], ['$set' => ['status' => AUDIT_STATUS_ERROR, _UPDATED_TS => $mongoDate, 'error' => 'Some unexpected error occurred.']])
            ->willReturn($mockUpdate);

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing', ['defaultContext' => 'http://talisaspire.com/']])
            ->getMock();
        $tripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['unlockAllDocuments', 'generateIdForNewMongoDocument', 'getMongoDate', 'getAuditManualRollbacksCollection'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $tripodUpdate->expects($this->once())
            ->method('generateIdForNewMongoDocument')
            ->willReturn($mongoDocumentId);

        $tripodUpdate->expects($this->exactly(2))
            ->method('getMongoDate')
            ->willReturn($mongoDate);

        $tripodUpdate->expects($this->once())
            ->method('getAuditManualRollbacksCollection')
            ->willReturn($auditManualRollbackCollection);

        $tripodUpdate->expects($this->once())
            ->method('unlockAllDocuments')
            ->willThrowException(new Exception('Some unexpected error occurred.'));

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdate);

        $tripod->removeInertLocks('transaction_400', 'Unit tests');
    }

    public function testRemoveInertLocksUnlockSuccessfulVerifyAuditLog(): void
    {
        $subject = 'http://basedata.com/b/1';
        $subject2 = 'tenantData:1';
        $this->lockDocument($subject, 'transaction_400');
        $this->lockDocument($subject2, 'transaction_400');

        $mongoDocumentId = new ObjectId();
        $mongoDate = DateUtil::getMongoDate();

        $auditManualRollbackCollection = $this->getMockBuilder(Collection::class)
            ->onlyMethods(['insertOne', 'updateOne'])
            ->disableOriginalConstructor()
            ->getMock();

        $mockInsert = $this->getMockBuilder(InsertOneResult::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['isAcknowledged'])
            ->getMock();
        $mockInsert
            ->expects($this->once())
            ->method('isAcknowledged')
            ->willReturn(true);

        $mockUpdate = $this->getMockBuilder(UpdateResult::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['isAcknowledged'])
            ->getMock();
        $mockUpdate
            ->expects($this->once())
            ->method('isAcknowledged')
            ->willReturn(true);

        $auditManualRollbackCollection->expects($this->once())
            ->method('insertOne')
            ->with([
                '_id' => $mongoDocumentId,
                'type' => AUDIT_TYPE_REMOVE_INERT_LOCKS,
                'status' => AUDIT_STATUS_IN_PROGRESS,
                'reason' => 'Unit tests',
                'transaction_id' => 'transaction_400',
                'documents' => ['baseData:1', 'tenantData:1'],
                _CREATED_TS => $mongoDate,
            ])
            ->willReturn($mockInsert);

        $auditManualRollbackCollection->expects($this->once())
            ->method('updateOne')
            ->with(['_id' => $mongoDocumentId], ['$set' => ['status' => AUDIT_STATUS_COMPLETED, _UPDATED_TS => $mongoDate]])
            ->willReturn($mockUpdate);

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing', ['defaultContext' => 'http://talisaspire.com/']])
            ->getMock();
        $tripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['unlockAllDocuments', 'generateIdForNewMongoDocument', 'getMongoDate', 'getAuditManualRollbacksCollection'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $tripodUpdate->expects($this->once())
            ->method('generateIdForNewMongoDocument')
            ->willReturn($mongoDocumentId);

        $tripodUpdate->expects($this->once())
            ->method('getAuditManualRollbacksCollection')
            ->willReturn($auditManualRollbackCollection);

        $tripodUpdate->expects($this->exactly(2))
            ->method('getMongoDate')
            ->willReturn($mongoDate);

        $tripodUpdate->expects($this->once())
            ->method('unlockAllDocuments')
            ->willReturn(true);

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdate);

        $this->assertTrue($tripod->removeInertLocks('transaction_400', 'Unit tests'));
    }

    public function testRemoveInertLocks(): void
    {
        $subject = 'http://basedata.com/b/1';
        $this->lockDocument($subject, 'transaction_100');

        $this->tripod->removeInertLocks('transaction_100', 'Unit tests');
        $docs = $this->tripod->getLockedDocuments();
        $this->assertCount(0, $docs);
    }

    public function testStatsD(): void
    {
        $mockStatsD = $this->getMockBuilder(StatsD::class)
            ->onlyMethods(['send'])
            ->setConstructorArgs(['localhost', '2012', 'myapp'])
            ->getMock();
        $mockStatsD->setPivotValue('tripod_php_testing');

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getStat'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing', ['defaultContext' => 'http://talisaspire.com/', 'statsDHost' => 'localhost', 'statsDPort' => '2012', 'statsDPrefix' => 'myapp']])
            ->getMock();
        $mockTripod
            ->method('getStat')
            ->willReturn($mockStatsD);

        $mockStatsD->expects($this->once())
            ->method('send')
            ->with(
                [
                    'myapp.tripod.MONGO_GET_ETAG' => '1|c',
                ]
            );

        $mockTripod->getETag('http://foo');
    }

    /** END: removeInertLocks tests */

    /** START: saveChangesHooks tests */
    public function testRegisteredHooksAreCalled(): void
    {
        $mockHookA = $this->getMockBuilder(TestSaveChangesHookA::class)
            ->onlyMethods(['pre', 'success', 'failure'])
            ->disableOriginalConstructor()
            ->getMock();
        $mockHookB = $this->getMockBuilder(TestSaveChangesHookB::class)
            ->onlyMethods(['pre', 'success', 'failure'])
            ->disableOriginalConstructor()
            ->getMock();

        $mockHookA->expects($this->once())->method('pre');
        $mockHookA->expects($this->once())->method('success');
        $mockHookA->expects($this->never())->method('failure');
        $mockHookB->expects($this->once())->method('pre');
        $mockHookB->expects($this->once())->method('success');
        $mockHookB->expects($this->never())->method('failure');

        $this->tripod->registerHook(IEventHook::EVENT_SAVE_CHANGES, $mockHookA);
        $this->tripod->registerHook(IEventHook::EVENT_SAVE_CHANGES, $mockHookB);

        $this->tripod->saveChanges(new ExtendedGraph(), new ExtendedGraph());
    }

    public function testRegisteredSuccessHooksAreNotCalledOnException(): void
    {
        $this->expectException(Tripod\Exceptions\Exception::class);
        $this->expectExceptionMessage('Could not validate');

        $tripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['validateGraphCardinality'])
            ->setConstructorArgs([$this->tripod])
            ->getMock();

        $mockHookA = $this->getMockBuilder(TestSaveChangesHookA::class)
            ->onlyMethods(['pre', 'success', 'failure'])
            ->disableOriginalConstructor()
            ->getMock();

        $mockHookB = $this->getMockBuilder(TestSaveChangesHookB::class)
            ->onlyMethods(['pre', 'success', 'failure'])
            ->disableOriginalConstructor()
            ->getMock();

        $mockHookA->expects($this->once())->method('pre');
        $mockHookA->expects($this->never())->method('success');
        $mockHookA->expects($this->once())->method('failure');
        $mockHookB->expects($this->once())->method('pre');
        $mockHookB->expects($this->never())->method('success');
        $mockHookB->expects($this->once())->method('failure');

        $tripodUpdate->registerSaveChangesEventHook($mockHookA);
        $tripodUpdate->registerSaveChangesEventHook($mockHookB);

        $tripodUpdate->expects($this->once())->method('validateGraphCardinality')->willThrowException(new Tripod\Exceptions\Exception('Could not validate'));
        $tripodUpdate->saveChanges(new ExtendedGraph(), new ExtendedGraph());
    }

    public function testMisbehavingHookDoesNotPreventSaveOrInterfereWithOtherHooks(): void
    {
        $mockHookA = $this->getMockBuilder(TestSaveChangesHookA::class)
            ->onlyMethods(['pre', 'success', 'failure'])
            ->disableOriginalConstructor()
            ->getMock();
        $mockHookB = $this->getMockBuilder(TestSaveChangesHookB::class)
            ->onlyMethods(['pre', 'success', 'failure'])
            ->disableOriginalConstructor()
            ->getMock();

        $mockHookA->expects($this->once())->method('pre')->willThrowException(new Exception('Misbehaving hook'));
        $mockHookA->expects($this->once())->method('success')->willThrowException(new Exception('Misbehaving hook'));
        $mockHookA->expects($this->never())->method('failure');
        $mockHookB->expects($this->once())->method('pre');
        $mockHookB->expects($this->once())->method('success');
        $mockHookB->expects($this->never())->method('failure');

        $this->tripod->registerHook(IEventHook::EVENT_SAVE_CHANGES, $mockHookA);
        $this->tripod->registerHook(IEventHook::EVENT_SAVE_CHANGES, $mockHookB);

        $this->tripod->saveChanges(new ExtendedGraph(), new ExtendedGraph());
    }

    /** END: saveChangesHooks tests */
    public function testPassStatConfigToTripodConstructor(): void
    {
        $statsDConfig = $this->getStatsDConfig();
        $opts = ['statsConfig' => $statsDConfig];

        $mockStat = $this->getMockStat($opts['statsConfig']['config']['host'], $opts['statsConfig']['config']['port'], $opts['statsConfig']['config']['prefix']);
        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getStatFromStatFactory'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing', $opts])
            ->getMock();

        $tripod->expects($this->once())
            ->method('getStatFromStatFactory')
            ->willReturn($mockStat);

        /** @var StatsD */
        $stat = $tripod->getStat();

        $this->assertInstanceOf(StatsD::class, $stat);

        $this->assertEquals('example.com', $stat->getHost());
        $this->assertEquals(1234, $stat->getPort());
        $this->assertSame('somePrefix', $stat->getPrefix());

        $config = $stat->getConfig();
        $this->assertEquals(
            [
                'host' => 'example.com',
                'port' => 1234,
                'prefix' => 'somePrefix',
            ],
            $config['config']
        );

        $cleanConfig = $tripod->getStatsConfig();
        $this->assertEquals(
            [
                'host' => 'example.com',
                'port' => 1234,
                'prefix' => 'somePrefix',
            ],
            $cleanConfig['config']
        );
    }

    /** START: getETag tests */
    public function testEtagIsMicrotimeFormat(): void
    {
        $config = Config::getInstance();
        $updatedAt = DateUtil::getMongoDate();

        $_id = [
            'r' => 'http://talisaspire.com/resources/testEtag',
            'c' => 'http://talisaspire.com/',
        ];
        $doc = [
            '_id' => $_id,
            'dct:title' => ['l' => 'etag'],
            '_version' => 0,
            '_cts' => $updatedAt,
            '_uts' => $updatedAt,
        ];
        $config->getCollectionForCBD(
            'tripod_php_testing',
            'CBD_testing'
        )->insertOne($doc, ['w' => 1]);

        $tripod = new Driver('CBD_testing', 'tripod_php_testing', ['defaultContext' => 'http://talisaspire.com/']);
        $this->assertMatchesRegularExpression('/^0.\d{8} \d{10}/', $tripod->getETag($_id['r']));
    }

    // END: getETag tests
}

class TestSaveChangesHookA implements IEventHook
{
    /**
     * This method gets called just before the event happens. The arguments passed depend on the event in question, see
     * the documentation for that event type for details.
     *
     * @param $args array of arguments
     */
    public function pre(array $args): void
    {
        // do nothing
    }

    /**
     * This method gets called after the event has successfully completed. The arguments passed depend on the event in
     * question, see the documentation for that event type for details
     * If the event throws an exception or fatal error, this method will not be called.
     *
     * @param $args array of arguments
     */
    public function success(array $args): void
    {
        // do nothing
    }

    /**
     * This method gets called if the event failed for any reason. The arguments passed should be the same as IEventHook::pre.
     */
    public function failure(array $args): void
    {
        // do nothing
    }
}

class TestSaveChangesHookB extends TestSaveChangesHookA
{
    // empty
}

/** END: saveChangesHooks tests */
class TripodDriverTestConfig extends Tripod\Mongo\Config
{
    /**
     * Constructor.
     */
    public function __construct() {}

    protected function loadConfig(array $config): void
    {
        parent::loadConfig($config);
    }
}
