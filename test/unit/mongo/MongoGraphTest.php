<?php

declare(strict_types=1);

use Tripod\Config;
use Tripod\Exceptions\Exception;
use Tripod\Exceptions\LabellerException;
use Tripod\Mongo\MongoGraph;

class MongoGraphTest extends MongoTripodTestBase
{
    protected function setUp(): void
    {
        parent::setup();
    }

    public function testUriToQNameOnRegisteredNS(): void
    {
        $g = new MongoGraph();
        $this->assertSame('dct:title', $g->uri_to_qname('http://purl.org/dc/terms/title'));
    }

    public function testUriToQNameOnUnRegisteredNS(): void
    {
        $this->expectException(LabellerException::class);
        $this->expectExceptionMessage('Could not label: http://someunregisteredns/');
        $g = new MongoGraph();
        $g->uri_to_qname('http://someunregisteredns/title');
    }

    public function testQNameToUriOnUnRegisteredNS(): void
    {
        $this->expectException(LabellerException::class);
        $this->expectExceptionMessage('Could not label: someunregisteredns:title');
        $g = new MongoGraph();
        $g->qname_to_uri('someunregisteredns:title');
    }

    public function testToNQuadsThrowsInvalidArgumentException(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('You must specify the context when serializing to nquads');
        $g = new MongoGraph();
        $g->to_nquads(null);
    }

    public function testToNQuads(): void
    {
        $g = new MongoGraph();
        $g->add_literal_triple('http://example.com/1', $g->qname_to_uri('dct:title'), 'some literal title');
        $g->add_resource_triple('http://example.com/1', $g->qname_to_uri('dct:source'), 'http://www.google.com');

        $expected = "<http://example.com/1> <http://purl.org/dc/terms/title> \"some literal title\" <http://talisaspire.com/> .
<http://example.com/1> <http://purl.org/dc/terms/source> <http://www.google.com> <http://talisaspire.com/> .\n";
        $this->assertSame($expected, $g->to_nquads(Config::getInstance()->getDefaultContextAlias()));
    }

    public function testToNQuadsTwoGraphsWithDifferentContext(): void
    {
        $g = new MongoGraph();
        $g->add_literal_triple('http://example.com/1', $g->qname_to_uri('dct:title'), 'some literal title');
        $g->add_resource_triple('http://example.com/1', $g->qname_to_uri('dct:source'), 'http://www.google.com');

        $expected = "<http://example.com/1> <http://purl.org/dc/terms/title> \"some literal title\" <http://talisaspire.com/> .
<http://example.com/1> <http://purl.org/dc/terms/source> <http://www.google.com> <http://talisaspire.com/> .\n";
        $this->assertSame($expected, $g->to_nquads('http://talisaspire.com/'));

        $g = new MongoGraph();
        $g->add_literal_triple('http://example.com/2', $g->qname_to_uri('dct:title'), 'some literal title');
        $g->add_resource_triple('http://example.com/2', $g->qname_to_uri('dct:source'), 'http://www.google.com');

        $expected = "<http://example.com/2> <http://purl.org/dc/terms/title> \"some literal title\" <http://wibble.talisaspire.com/> .
<http://example.com/2> <http://purl.org/dc/terms/source> <http://www.google.com> <http://wibble.talisaspire.com/> .\n";
        $this->assertSame($expected, $g->to_nquads('http://wibble.talisaspire.com/'));
    }

    public function testAddTripodArraySingleDoc(): void
    {
        $doc = [
            '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6-2', 'c' => 'http://talisaspire.com/works/4d101f63c10a6-2'],
            '_version' => 0,
            'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
            'rdf:type' => [
                ['u' => 'http://purl.org/ontology/bibo/Book'],
                ['u' => 'http://talisaspire.com/schema#Work'],
            ],
            'bibo:isbn13' => ['l' => '9211234567890'],
        ];

        $expected = new MongoGraph();
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('bibo:isbn13'), '9211234567890');
        $expected->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('dct:subject'), 'http://talisaspire.com/disciplines/physics');
        $expected->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), 'http://purl.org/ontology/bibo/Book');
        $expected->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), 'http://talisaspire.com/schema#Work');

        $g = new MongoGraph();
        $g->add_tripod_array($doc);

        $this->assertEquals($expected, $g);
    }

    /**
     * @dataProvider addTripodArrayContainingValidLiteralValues_Provider
     *
     * @param bool|float|int|string $value
     */
    public function testAddTripodArrayContainingValidLiteralValues($value): void
    {
        $doc = [
            '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6-2', 'c' => 'http://talisaspire.com/works/4d101f63c10a6-2'],
            '_version' => 0,
            'rdf:type' => [
                ['l' => $value],
                ['l' => 'a Value'],
            ],
            'bibo:isbn13' => ['l' => '9211234567890'],
            'bibo:isbn10' => ['l' => $value],
        ];

        $expected = new MongoGraph();
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('bibo:isbn13'), '9211234567890');
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('bibo:isbn10'), $value);
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), $value);
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), 'a Value');

        $g = new MongoGraph();
        $g->add_tripod_array($doc);

        $this->assertEquals($expected, $g);
    }

    public function addTripodArrayContainingValidLiteralValues_Provider(): iterable
    {
        yield ['A String'];
        yield [''];
        yield ['0'];
        yield [1];
        yield [1.2];
        yield [true];
    }

    /**
     * @dataProvider addTripodArrayContainingInvalidLiteralValues_Provider
     *
     * @param mixed $value
     */
    public function testAddTripodArrayContainingInvalidLiteralValues($value): void
    {
        $doc = [
            '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6-2', 'c' => 'http://talisaspire.com/works/4d101f63c10a6-2'],
            '_version' => 0,
            'rdf:type' => [
                ['l' => $value],
                ['l' => 'a Value'],
            ],
            'bibo:isbn13' => ['l' => '9211234567890'],
            'bibo:isbn10' => ['l' => $value],
        ];

        $expected = new MongoGraph();
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('bibo:isbn13'), '9211234567890');
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), 'a Value');

        $g = new MongoGraph();
        $g->add_tripod_array($doc);

        $this->assertEquals($expected, $g);
    }

    public function addTripodArrayContainingInvalidLiteralValues_Provider(): iterable
    {
        yield [null];
        yield [new stdClass()];
        yield [function (): void {}];
    }

    /**
     * @dataProvider addTripodArrayContainingInvalidPredicates_Provider
     *
     * @param bool|int|string $value
     */
    public function testAddTripodArrayContainingInvalidPredicates($value): void
    {
        $this->expectExceptionMessage('The predicate must be a non-empty string');
        $doc = [
            '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6-2', 'c' => 'http://talisaspire.com/works/4d101f63c10a6-2'],
            '_version' => 0,
            'rdf:type' => [
                ['l' => 'a Value'],
            ],
            'bibo:isbn13' => ['l' => '9211234567890'],
            $value => ['l' => '9211234567890'],
        ];

        $expected = new MongoGraph();
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('bibo:isbn13'), '9211234567890');
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), 'a Value');

        $g = new MongoGraph();
        $g->add_tripod_array($doc);
    }

    public function addTripodArrayContainingInvalidPredicates_Provider(): iterable
    {
        yield [''];
        yield ['0'];
        yield [1];
        yield [true];
    }

    /**
     * We are expecting the labeller.
     */
    public function testAddTripodArrayContainingEmptyPredicate(): void
    {
        // Should not be able to label ''
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('The predicate must be a non-empty string');
        $doc = [
            '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6-2', 'c' => 'http://talisaspire.com/works/4d101f63c10a6-2'],
            '_version' => 0,
            'rdf:type' => [
                ['l' => 'a Value'],
            ],
            'bibo:isbn13' => ['l' => '9211234567890'],
            '' => ['l' => '9211234567890'],
        ];

        $expected = new MongoGraph();
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('bibo:isbn13'), '9211234567890');
        $expected->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), 'a Value');

        $g = new MongoGraph();
        $g->add_tripod_array($doc);
    }

    /**
     * @dataProvider addTripodArrayContainingInvalidSubject_Provider
     *
     * @param mixed $value
     */
    public function testAddTripodArrayContainingInvalidSubject($value): void
    {
        $this->expectException(Exception::class);
        $doc = [
            '_id' => ['r' => $value, 'c' => 'http://talisaspire.com/works/4d101f63c10a6-2'],
            '_version' => 0,
            'rdf:type' => [
                ['l' => 'a Value'],
            ],
            'bibo:isbn13' => ['l' => '9211234567890'],
        ];

        $g = new MongoGraph();
        $g->add_tripod_array($doc);
    }

    public function addTripodArrayContainingInvalidSubject_Provider(): iterable
    {
        yield [''];
        yield ['0'];
        yield [1];
        yield [1.2];
        yield [true];
    }

    public function testAddTripodArrayContainingValidResourceValues(): void
    {
        $value = 'A String';
        $doc = [
            '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6-2', 'c' => 'http://talisaspire.com/works/4d101f63c10a6-2'],
            '_version' => 0,
            'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
            'dct:publisher' => ['u' => $value],
            'rdf:type' => [
                ['u' => $value],
                ['u' => 'http://talisaspire.com/schema#Work'],
            ],
        ];

        $expected = new MongoGraph();
        $expected->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('dct:subject'), 'http://talisaspire.com/disciplines/physics');
        $expected->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('dct:publisher'), $value);
        $expected->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), $value);
        $expected->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), 'http://talisaspire.com/schema#Work');

        $g = new MongoGraph();
        $g->add_tripod_array($doc);

        $this->assertEquals($expected, $g);
    }

    /**
     * @dataProvider addTripodArrayContainingInvalidResourceValues_Provider
     *
     * @param mixed $value
     */
    public function testAddTripodArrayContainingInvalidResourceValues($value): void
    {
        $doc = [
            '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6-2', 'c' => 'http://talisaspire.com/works/4d101f63c10a6-2'],
            '_version' => 0,
            'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
            'dct:publisher' => ['u' => $value],
            'rdf:type' => [
                ['u' => $value],
                ['u' => 'http://talisaspire.com/schema#Work'],
            ],
        ];

        $expected = new MongoGraph();
        $expected->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('dct:subject'), 'http://talisaspire.com/disciplines/physics');
        $expected->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $expected->qname_to_uri('rdf:type'), 'http://talisaspire.com/schema#Work');

        $g = new MongoGraph();
        $g->add_tripod_array($doc);

        $this->assertEquals($expected, $g);
    }

    public function addTripodArrayContainingInvalidResourceValues_Provider(): iterable
    {
        yield [1];
        yield [1.2];
        yield [''];
        yield ['0'];
        yield [true];
        yield [[]];
        yield [null];
        yield [new stdClass()];
        yield [function (): void {}];
    }

    public function testAddTripodArrayWhenAddingViews(): void
    {
        // view contains 4 subgraphs
        $view = $this->decodeJsonFile(__DIR__ . '/data/view.json');
        $g = new MongoGraph();
        $g->add_tripod_array($view);

        // graph should contain 4 subgraphs
        $this->assertCount(4, $g->get_subjects());

        // assert each subgraph
        $this->assertHasLiteralTriple($g, 'http://example.com/resources/1', $g->qname_to_uri('dct:date'), '2003');
        $this->assertHasLiteralTriple($g, 'http://example.com/resources/1', $g->qname_to_uri('dct:title'), 'Material cultures: why some things matter');
        $this->assertHasLiteralTriple($g, 'http://example.com/resources/1', $g->qname_to_uri('bibo:isbn10'), '1857286863');
        $this->assertHasLiteralTriple($g, 'http://example.com/resources/1', $g->qname_to_uri('bibo:volume'), 'Consumption and space');
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1', $g->qname_to_uri('dct:publisher'), 'http://example.com/organisations/1');
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1', $g->qname_to_uri('dct:source'), 'http://example.com/catalog/542114');
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1', $g->qname_to_uri('bibo:authorList'), 'http://example.com/resources/1/authors');
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1', $g->qname_to_uri('rdf:type'), 'http://purl.org/ontology/bibo/Book');

        $this->assertHasLiteralTriple($g, 'http://example.com/people/1', $g->qname_to_uri('foaf:name'), 'Miller, Daniel');
        $this->assertHasResourceTriple($g, 'http://example.com/people/1', $g->qname_to_uri('rdf:type'), 'http://xmlns.com/foaf/0.1/Person');

        $this->assertHasLiteralTriple($g, 'http://example.com/organisations/1', $g->qname_to_uri('foaf:name'), 'Routledge');
        $this->assertHasResourceTriple($g, 'http://example.com/organisations/1', $g->qname_to_uri('rdf:type'), 'http://xmlns.com/foaf/0.1/Organization');

        $this->assertHasResourceTriple($g, 'http://example.com/resources/1/authors', $g->qname_to_uri('rdf:_1'), 'http://example.com/people/1');
        $this->assertHasResourceTriple($g, 'http://example.com/resources/1/authors', $g->qname_to_uri('rdf:type'), 'http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq');
    }

    public function testToTripodArray(): void
    {
        $expected = [
            '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6-2', 'c' => 'http://example.com/'],
            'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
            'rdf:type' => [
                ['u' => 'bibo:Book'],
                ['u' => 'acorn:Work'],
            ],
            'bibo:isbn13' => ['l' => '9211234567890'],
        ];

        // create a graph adding properties to it
        $g = new MongoGraph();
        $g->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6-2', $g->qname_to_uri('bibo:isbn13'), '9211234567890');
        $g->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $g->qname_to_uri('dct:subject'), 'http://talisaspire.com/disciplines/physics');
        $g->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $g->qname_to_uri('rdf:type'), 'http://purl.org/ontology/bibo/Book');
        $g->add_resource_triple('http://talisaspire.com/works/4d101f63c10a6-2', $g->qname_to_uri('rdf:type'), 'http://talisaspire.com/schema#Work');

        $actual = $g->to_tripod_array('http://talisaspire.com/works/4d101f63c10a6-2', 'http://example.com/');

        $this->assertEquals($expected, $actual);
    }

    public function testToTripodArrayReturnsNullIfDocNotInGraph(): void
    {
        $g = new MongoGraph();
        $doc = $g->to_tripod_array('http://example.com/1', 'http://example.com/');
        $this->assertNull($doc);
    }

    public function testToTripodViewArray(): void
    {
        $expected = [
            '_id' => ['r' => 'http://example.com/things/1', 'c' => 'http://example.com/'],
            'value' => [
                '_impactIndex' => ['http://example.com/things/1', 'http://example.com/things/2'],
                '_graphs' => [
                    [
                        '_id' => ['r' => 'http://example.com/things/1', 'c' => 'http://example.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => ['u' => 'bibo:Book'],
                        'bibo:isbn13' => ['l' => '9211234567890'],
                    ],
                    [
                        '_id' => ['r' => 'http://example.com/things/2', 'c' => 'http://example.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/maths'],
                        'rdf:type' => ['u' => 'acorn:Work'],
                    ],
                ],
            ],
        ];

        // create a graph adding properties to it
        $g = new MongoGraph();
        $g->add_resource_triple('http://example.com/things/1', $g->qname_to_uri('dct:subject'), 'http://talisaspire.com/disciplines/physics');
        $g->add_resource_triple('http://example.com/things/1', $g->qname_to_uri('rdf:type'), 'http://purl.org/ontology/bibo/Book');
        $g->add_literal_triple('http://example.com/things/1', $g->qname_to_uri('bibo:isbn13'), '9211234567890');
        $g->add_resource_triple('http://example.com/things/2', $g->qname_to_uri('dct:subject'), 'http://talisaspire.com/disciplines/maths');
        $g->add_resource_triple('http://example.com/things/2', $g->qname_to_uri('rdf:type'), 'http://talisaspire.com/schema#Work');

        $actual = $g->to_tripod_view_array('http://example.com/things/1', 'http://example.com/');
        $this->assertSame($expected, $actual);
    }

    public function testWriteLockedDocDoesNotExposeVersionOrLockPropertyInGraph(): void
    {
        $doc = [
            '_id' => ['r' => 'http://example.com/things/1', 'c' => 'http://example.com/'],
            _VERSION => 0,
            _LOCKED_FOR_TRANS => 'transaction_234',
        ];

        $g = new MongoGraph();
        $g->add_tripod_array($doc);
        $this->assertCount(0, $g->get_index(), 'Graph should contain no data');
    }
}
