<?php

declare(strict_types=1);

use Tripod\Config;
use Tripod\Mongo\TriplesUtil;

class TriplesUtilTest extends MongoTripodTestBase
{
    private const SUBJECT = 'http://serials.talisaspire.com/issn/0893-0465';

    private const CONTEXT = 'http://talisaspire.com/';

    public function testGetTArrayAbout(): void
    {
        $tu = new TriplesUtil();
        $this->assertSame($this->getExpectedDoc(), $tu->getTArrayAbout(self::SUBJECT, $this->getTriples(), self::CONTEXT));
    }

    public function testGetTArrayAboutWithLiteralContainingSpaces(): void
    {
        $tu = new TriplesUtil();
        $triples = ['<http://example.com/1> <http://purl.org/dc/terms/title> "A title with spaces" . '];
        $doc = $tu->getTArrayAbout('http://example.com/1', $triples, self::CONTEXT);
        $this->assertNotNull($doc);
        $this->assertSame('A title with spaces', $doc['dct:title']['l']);
    }

    public function testGetTArrayAboutWithTypedLiteral(): void
    {
        $tu = new TriplesUtil();
        $triples = ['<http://example.com/1> <http://purl.org/dc/terms/extent> "123"^^<http://www.w3.org/2001/XMLSchema#integer> . '];
        $doc = $tu->getTArrayAbout('http://example.com/1', $triples, self::CONTEXT);
        $this->assertNotNull($doc);
        $this->assertSame('123', $doc['dct:extent']['l']);
    }

    public function testBsonizeTriplesAbout(): void
    {
        $tu = new TriplesUtil();
        $this->assertSame($this->getExpectedDoc(), $tu->bsonizeTriplesAbout(self::SUBJECT, $this->getTriples(), self::CONTEXT));
    }

    public function testBsonizeTriplesAboutDefaultsToDefaultContext(): void
    {
        $tu = new TriplesUtil();
        $doc = $tu->bsonizeTriplesAbout(self::SUBJECT, $this->getTriples());
        $this->assertNotNull($doc);
        $this->assertSame(Config::getInstance()->getDefaultContextAlias(), $doc['_id']['c']);
    }

    public function testExtractMissingPredicateNs(): void
    {
        $tu = new TriplesUtil();
        $triples = [
            '<http://example.com/1> <http://purl.org/dc/terms/title> "known ns" . ',
            '<http://example.com/1> <http://unknown.example.org/vocab#pred> <http://example.com/2> . ',
            '<http://example.com/1> <http://unknown.example.org/vocab#otherPred> <http://example.com/3> . ',
        ];

        $this->assertSame(['http://unknown.example.org/vocab#'], $tu->extractMissingPredicateNs($triples));
    }

    public function testExtractMissingObjectNs(): void
    {
        $tu = new TriplesUtil();
        $triples = [
            '<http://example.com/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://unknown.example.org/vocab#Thing> . ',
            '<http://example.com/1> <http://purl.org/dc/terms/title> "a literal is not checked" . ',
        ];

        $this->assertSame(['http://unknown.example.org/vocab#'], $tu->extractMissingObjectNs($triples));
    }

    public function testSuggestPrefix(): void
    {
        $tu = new TriplesUtil();
        $this->assertSame('myvocab', $tu->suggestPrefix('http://example.com/myvocab/'));
        // reserved parts are skipped in favour of earlier URI parts
        $this->assertSame('alpha', $tu->suggestPrefix('http://example.org/alpha/schema#'));
        // no eligible part falls back to a generated prefix
        $this->assertStringStartsWith('unknown', $tu->suggestPrefix('http://foo.io/ns#'));
    }

    public function testLoadTriplesAbout(): void
    {
        $collection = Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing');
        $collection->deleteMany(['_id.r' => self::SUBJECT]);

        $tu = new TriplesUtil();
        $tu->loadTriplesAbout(self::SUBJECT, $this->getTriples(), 'tripod_php_testing', 'CBD_testing', self::CONTEXT);

        $doc = $collection->findOne(['_id' => ['r' => self::SUBJECT, 'c' => self::CONTEXT]]);
        $this->assertNotNull($doc);
        $this->assertEquals('bibo:Journal', $doc['rdf:type']['u']);
        $this->assertEquals('1548-774X', $doc['bibo:eissn']['l']);
    }

    public function testLoadTriplesAboutMergesIntoExistingDocument(): void
    {
        $collection = Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing');
        $collection->deleteMany(['_id.r' => self::SUBJECT]);

        $tu = new TriplesUtil();
        $tu->loadTriplesAbout(self::SUBJECT, $this->getTriples(), 'tripod_php_testing', 'CBD_testing', self::CONTEXT);
        $tu->loadTriplesAbout(
            self::SUBJECT,
            ['<http://serials.talisaspire.com/issn/0893-0465> <http://purl.org/ontology/bibo/issn> "0893-0465" . '],
            'tripod_php_testing',
            'CBD_testing',
            self::CONTEXT
        );

        $doc = $collection->findOne(['_id' => ['r' => self::SUBJECT, 'c' => self::CONTEXT]]);
        $this->assertNotNull($doc);
        // the original data and the merged-in triple are both present
        $this->assertEquals('1548-774X', $doc['bibo:eissn']['l']);
        $this->assertEquals('0893-0465', $doc['bibo:issn']['l']);
    }

    public function testLoadTriplesAboutWithMatchingAllowableType(): void
    {
        $collection = Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing');
        $collection->deleteMany(['_id.r' => self::SUBJECT]);

        $tu = new TriplesUtil();
        $tu->loadTriplesAbout(
            self::SUBJECT,
            $this->getTriples(),
            'tripod_php_testing',
            'CBD_testing',
            self::CONTEXT,
            ['http://purl.org/ontology/bibo/Journal']
        );

        $this->assertNotNull($collection->findOne(['_id' => ['r' => self::SUBJECT, 'c' => self::CONTEXT]]));
    }

    public function testLoadTriplesAboutWithNonMatchingAllowableTypeDoesNotSave(): void
    {
        $collection = Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing');
        $collection->deleteMany(['_id.r' => self::SUBJECT]);

        $tu = new TriplesUtil();
        $tu->loadTriplesAbout(
            self::SUBJECT,
            $this->getTriples(),
            'tripod_php_testing',
            'CBD_testing',
            self::CONTEXT,
            ['http://purl.org/ontology/bibo/Book']
        );

        $this->assertNull($collection->findOne(['_id' => ['r' => self::SUBJECT, 'c' => self::CONTEXT]]));
    }

    /**
     * @return string[]
     */
    private function getTriples(): array
    {
        $triples = [];
        $triples[] = '<http://serials.talisaspire.com/issn/0893-0465> <http://xmlns.com/foaf/0.1/page> <http://www.ingentaconnect.com/content/bpl/ciso> . ';
        $triples[] = '<http://serials.talisaspire.com/issn/0893-0465> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Journal> . ';
        $triples[] = '<http://serials.talisaspire.com/issn/0893-0465> <http://xmlns.com/foaf/0.1/page> <http://onlinelibrary.wiley.com/journal/10.1111/(ISSN)1548-744X> . ';
        $triples[] = '<http://serials.talisaspire.com/issn/0893-0465> <http://purl.org/ontology/bibo/eissn> "1548-774X" . ';

        return $triples;
    }

    private function getExpectedDoc(): array
    {
        return [
            '_id' => ['r' => self::SUBJECT, 'c' => self::CONTEXT],
            'foaf:page' => [
                [
                    'u' => 'http://www.ingentaconnect.com/content/bpl/ciso',
                ],
                [
                    'u' => 'http://onlinelibrary.wiley.com/journal/10.1111/(ISSN)1548-744X',
                ],
            ],
            'rdf:type' => [
                'u' => 'bibo:Journal',
            ],
            'bibo:eissn' => [
                'l' => '1548-774X',
            ],
        ];
    }
}
