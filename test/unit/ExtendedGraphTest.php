<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Tripod\ExtendedGraph;

class ExtendedGraphTest extends TestCase
{
    private const ONT_foaf = 'http://xmlns.com/foaf/0.1/';

    private const ONT_resource = 'http://purl.org/vocab/resourcelist/schema#';

    /**
     * @dataProvider addValidValueToLiteralResultsInTriple_Provider
     *
     * @param mixed $value
     */
    public function testAddValidValueToLiteralResultsInTriple($value): void
    {
        $graph = new ExtendedGraph();
        $addResult = $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', $value);
        $this->assertTrue($addResult, 'The triple should have been added for this value');

        $hasPropertyResult = $graph->subject_has_property('http://some/subject/1', 'http://some/predicate');
        $this->assertTrue($hasPropertyResult, 'The triple should have been added for this value');
    }

    public function addValidValueToLiteralResultsInTriple_Provider(): iterable
    {
        yield ['String'];
        yield [''];
        yield ['0'];
        yield [1];
        yield [1.2];
        yield [true];
    }

    /**
     * @dataProvider addInvalidValueToLiteralResultsInNoTriple_Provider
     *
     * @param mixed $value
     */
    public function testAddInvalidValueToLiteralResultsInNoTriple($value): void
    {
        $graph = new ExtendedGraph();
        $addResult = $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', $value);
        $this->assertFalse($addResult, 'The triple should not have been added for this value');

        $hasPropertyResult = $graph->subject_has_property('http://some/subject/1', 'http://some/predicate');
        $this->assertFalse($hasPropertyResult, 'The triple should not have been added for this value');
    }

    public function addInvalidValueToLiteralResultsInNoTriple_Provider(): iterable
    {
        yield [null];
        yield [new stdClass()];
        yield [function (): void {}];
    }

    /**
     * @dataProvider addInvalidSubjectToLiteralResultsInNoTriple_Provider
     *
     * @param mixed $value
     */
    public function testAddInvalidSubjectToLiteralThrowsException($value): void
    {
        $this->expectExceptionMessageMatches('/^The subject is invalid$|Argument #?1.+must be of (the )?type string.+/');

        $graph = new ExtendedGraph();
        $graph->add_resource_triple($value, 'http://some/predicate', 'http://someplace.com');
    }

    public function addInvalidSubjectToLiteralResultsInNoTriple_Provider(): iterable
    {
        yield [''];
        yield ['0'];
        yield [1];
        yield [1.2];
        yield [true];
        yield [[]];
        yield [null];
        yield [new stdClass()];
        yield [function (): void {}];
    }

    /**
     * @dataProvider addInvalidSubjectToLiteralResultsInNoTriple_Provider
     *
     * @param mixed $value
     */
    public function testAddInvalidPredicateToLiteralThrowsException($value): void
    {
        $this->expectExceptionMessageMatches('/^The predicate is invalid$|Argument #?2.+must be of (the )?type string.+/');

        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://some/subject/1', $value, 'http://someplace.com');
    }

    public function addInvalidPredicateToLiteralResultsInNoTriple_Provider(): iterable
    {
        yield [''];
        yield ['0'];
        yield [1];
        yield [1.2];
        yield [true];
        yield [[]];
        yield [null];
        yield [new stdClass()];
        yield [function (): void {}];
    }

    public function testAddValidValueToResourceResultsInTriple(): void
    {
        $value = 'A String';
        $graph = new ExtendedGraph();
        $addResult = $graph->add_resource_triple('http://some/subject/1', 'http://some/predicate', $value);
        $this->assertTrue($addResult, 'The triple should have been added for this value');

        $hasPropertyResult = $graph->subject_has_property('http://some/subject/1', 'http://some/predicate');
        $this->assertTrue($hasPropertyResult, 'The triple should have been added for this value');
    }

    /**
     * @dataProvider addInvalidValueToResourceResultsInNoTriple_Provider
     *
     * @param mixed $value
     */
    public function testAddInvalidValueToResourceResultsInNoTriple($value): void
    {
        $graph = new ExtendedGraph();

        try {
            $addResult = $graph->add_resource_triple('http://some/subject/1', 'http://some/predicate', $value);
        } catch (TypeError $typeError) {
            $addResult = false;
        }

        $this->assertFalse($addResult, 'The triple should not have been added for this value');

        $hasPropertyResult = $graph->subject_has_property('http://some/subject/1', 'http://some/predicate');
        $this->assertFalse($hasPropertyResult, 'The triple should not have been added for this value');
    }

    public function addInvalidValueToResourceResultsInNoTriple_Provider(): iterable
    {
        yield [''];
        yield ['0'];
        yield [1];
        yield [1.2];
        yield [true];
        yield [[]];
        yield [null];
        yield [new stdClass()];
        yield [function (): void {}];
    }

    /**
     * @dataProvider addInvalidSubjectToResourceResultsInNoTriple_Provider
     *
     * @param mixed $value
     */
    public function testAddInvalidSubjectToResourceThrowsException($value): void
    {
        $this->expectExceptionMessageMatches('/^The subject is invalid$|Argument #?1.+must be of (the )?type string.+/');

        $graph = new ExtendedGraph();
        $graph->add_resource_triple($value, 'http://some/predicate', 'http://someplace.com');
    }

    public function addInvalidSubjectToResourceResultsInNoTriple_Provider(): iterable
    {
        yield [''];
        yield ['0'];
        yield [1];
        yield [1.2];
        yield [true];
        yield [[]];
        yield [null];
        yield [new stdClass()];
        yield [function (): void {}];
    }

    /**
     * @dataProvider addInvalidSubjectToLiteralResultsInNoTriple_Provider
     *
     * @param mixed $value
     */
    public function testAddInvalidPredicateToResourceThrowsException($value): void
    {
        $this->expectExceptionMessageMatches('/^The predicate is invalid$|Argument #?2.+must be of (the )?type string.+/');

        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://some/subject/1', $value, 'http://someplace.com');
    }

    public function addInvalidPredicateToResourceResultsInNoTriple_Provider(): iterable
    {
        yield [''];
        yield ['0'];
        yield [1];
        yield [1.2];
        yield [true];
        yield [[]];
        yield [null];
        yield [new stdClass()];
        yield [function (): void {}];
    }

    public function testRemoveProperties(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate/to/remove', 'some object');
        $graph->add_resource_triple('http://some/subject/3', 'http://some/predicate/to/remove', 'http://some/other/object');
        $graph->add_literal_triple('http://some/subject/4', 'http://some/predicate', 'some object');

        $graph->remove_properties('http://some/predicate/to/remove');

        $this->assertFalse($graph->subject_has_property('http://some/subject/2', 'http://some/predicate/to/remove'), 'should have removed triple about subject 2');
        $this->assertFalse($graph->subject_has_property('http://some/subject/3', 'http://some/predicate/to/remove'), 'should have removed triple about subject 3');
    }

    public function testGetFirstResource(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'value 1');
        $graph->add_resource_triple('http://some/subject/1', 'http://some/predicate', 'http://value/2');

        $this->assertSame('http://value/2', $graph->get_first_resource('http://some/subject/1', 'http://some/predicate'), 'should have returned first resource');
        $this->assertEquals(null, $graph->get_first_resource('http://some/subject/2', 'http://other/predicate'), 'should have returned default value');
        $this->assertSame('my default', $graph->get_first_resource('http://some/subject/3', 'http://other/predicate', 'my default'), 'should have returned default value');
    }

    public function testRemoveResourceTriple(): void
    {
        $graph = new ExtendedGraph();

        // Add some triples
        $graph->add_resource_triple('http://some/subject/1', 'http://some/predicate', 'http://value/1');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://value/2');
        $this->assertSame(2, $graph->get_triple_count(), 'should have 2 triples');

        // Try to remove triples that don't exist
        $graph->remove_resource_triple('http://some/subject/3', 'http://some/predicate', 'http://value/3');
        $graph->remove_literal_triple('http://some/subject/3', 'http://some/predicate', 'value 3');
        $this->assertSame(2, $graph->get_triple_count(), 'should have 2 triples');

        // Remove a triple that does exist
        $graph->remove_resource_triple('http://some/subject/1', 'http://some/predicate', 'http://value/1');
        $this->assertSame(1, $graph->get_triple_count(), 'should have 1 triple');

        // Remove the last triple
        $graph->remove_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://value/2');
        $this->assertSame(0, $graph->get_triple_count(), 'should have 0 triples');
        $this->assertTrue($graph->is_empty(), 'should be empty');
        $this->assertSame([], $graph->get_index(), 'should have empty index');
    }

    public function testRemoveLiteralTriple(): void
    {
        $graph = new ExtendedGraph();

        // Add some triples
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'value 1');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'value 2');
        $this->assertSame(2, $graph->get_triple_count(), 'should have 2 triples');

        // Try to remove triples that don't exist
        $graph->remove_literal_triple('http://some/subject/3', 'http://some/predicate', 'value 3');
        $graph->remove_resource_triple('http://some/subject/3', 'http://some/predicate', 'http://value/3');
        $this->assertSame(2, $graph->get_triple_count(), 'should have 2 triples');

        // Remove a triple that does exist
        $graph->remove_literal_triple('http://some/subject/1', 'http://some/predicate', 'value 1');
        $this->assertSame(1, $graph->get_triple_count(), 'should have 1 triple');

        // Remove the last triple
        $graph->remove_literal_triple('http://some/subject/2', 'http://some/predicate', 'value 2');
        $this->assertSame(0, $graph->get_triple_count(), 'should have 0 triples');
        $this->assertTrue($graph->is_empty(), 'should be empty');
        $this->assertSame([], $graph->get_index(), 'should have empty index');
    }

    public function testGetResourceProperties(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_resource_triple('http://some/subject/1', 'http://some/predicate', 'http://value/1');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://value/2');
        $graph->add_resource_triple('http://some/subject/3', 'http://some/predicate', 'http://value/3');

        $values = $graph->get_resource_properties('http://some/predicate');

        $this->assertEquals($values, ['http://value/1', 'http://value/2', 'http://value/3'], 'should have returned 3 values');
    }

    public function testGetSubjectsWithPropertyValue(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_resource_triple('http://some/subject/1', 'http://some/predicate', 'http://value/1');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://value/2');
        $graph->add_resource_triple('http://some/subject/3', 'http://some/predicate', 'http://value/3');

        $graph->add_literal_triple('http://some/subject/2-with-literal', 'http://some/predicate', 'http://value/2');

        $subjects = $graph->get_subjects_with_property_value('http://some/predicate', 'http://value/2');

        $this->assertEquals($subjects, ['http://some/subject/2', 'http://some/subject/2-with-literal'], 'should have returned correct subject');
    }

    public function testGetSequenceValues(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_4', 'http://value/4');
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_2', 'http://value/2');
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_3', 'http://value/3');
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_5', 'http://value/5');
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_1', 'http://value/1');

        $expectedArray = ['http://value/1', 'http://value/2', 'http://value/3', 'http://value/4', 'http://value/5'];
        $this->assertSame($expectedArray, $graph->get_sequence_values('http://some/subject/1'));
    }

    public function testAddResourceToSequence(): void
    {
        $testSubject = 'http://some/subject/s1';
        $testObject1 = 'http://some/object/o1';
        $testObject2 = 'http://example.com/foo/bar';

        $graph = new ExtendedGraph();
        $graph->add_resource_to_sequence($testSubject, $testObject1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject1], $objects);

        $graph->add_resource_to_sequence($testSubject, $testObject2);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject1, $testObject2], $objects);

        $graph->add_resource_to_sequence('http://some/other/subject', 'http://some/other/object');

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject1, $testObject2], $objects);

        $objects = $graph->get_sequence_values('http://some/other/subject');
        $this->assertSame(['http://some/other/object'], $objects);
    }

    public function testAddResourceToSequenceInPosition(): void
    {
        $testSubject = 'http://some/subject/s1';
        $testObject1 = 'http://some/object/o1';
        $testObject2 = 'http://some/object/o2';
        $testObject3 = 'http://some/object/o3';
        $testObject4 = 'http://some/object/o4';
        $testObject5 = 'http://some/object/o5';

        $graph = new ExtendedGraph();
        $graph->add_resource_to_sequence_in_position($testSubject, $testObject1, 1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject1], $objects);

        $graph->add_resource_to_sequence_in_position($testSubject, $testObject2, 1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject2, $testObject1], $objects);

        $graph->add_resource_to_sequence_in_position($testSubject, $testObject3, 1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject3, $testObject2, $testObject1], $objects);

        $graph->add_resource_to_sequence_in_position($testSubject, $testObject4, 3);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject3, $testObject2, $testObject4, $testObject1], $objects);

        $graph->add_resource_to_sequence_in_position($testSubject, $testObject5, count($objects) + 1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject3, $testObject2, $testObject4, $testObject1, $testObject5], $objects);
    }

    public function testAddToSequenceInPositionAgain(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_resource_to_sequence('http://seq', 'http://item/1');
        $graph->add_resource_to_sequence('http://seq', 'http://item/2');
        $graph->add_resource_to_sequence('http://seq', 'http://item/3');
        $graph->add_resource_to_sequence('http://seq', 'http://item/4');

        $graph->add_resource_to_sequence_in_position('http://seq', 'http://item/new', 2);

        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf . '_1', 'http://item/1'));
        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf . '_2', 'http://item/new'));
        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf . '_3', 'http://item/2'));
        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf . '_4', 'http://item/3'));
        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf . '_5', 'http://item/4'));
    }

    public function testAddLiteralToSequence(): void
    {
        $testSubject = 'http://some/subject/s1';
        $testObject1 = 'foo1';
        $testObject2 = 'foo2';

        $graph = new ExtendedGraph();
        $graph->add_literal_to_sequence($testSubject, $testObject1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject1], $objects);

        $graph->add_literal_to_sequence($testSubject, $testObject2);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject1, $testObject2], $objects);

        $graph->add_literal_to_sequence('http://some/other/subject', 'bar');

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertSame([$testObject1, $testObject2], $objects);

        $objects = $graph->get_sequence_values('http://some/other/subject');
        $this->assertSame(['bar'], $objects);
    }

    public function testGetTripleCountWithNoParams(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');

        $expected = 3;
        $actual = $graph->get_triple_count();
        $this->assertSame($expected, $actual);
    }

    public function testGetTripleCountWithSubject(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');

        $expected = 1;
        $actual = $graph->get_triple_count('http://some/subject/1');
        $this->assertSame($expected, $actual);
    }

    public function testGetTripleCountWithPredicate(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');

        $expected = 3;
        $actual = $graph->get_triple_count(null, 'http://some/predicate');
        $this->assertSame($expected, $actual);
    }

    public function testGetTripleCountWithObject(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');

        $expected = 3;
        $actual = $graph->get_triple_count(null, null, 'some object');
        $this->assertSame($expected, $actual);
    }

    public function testGetTripleCountWithSubjectandPredicate(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'another object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');

        $expected = 2;
        $actual = $graph->get_triple_count('http://some/subject/1', 'http://some/predicate');
        $this->assertSame($expected, $actual);
    }

    public function testGetTripleCountWithSubjectPredicateAndObject(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'another object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');

        $expected = 1;
        $actual = $graph->get_triple_count('http://some/subject/1', 'http://some/predicate', 'some object');
        $this->assertSame($expected, $actual);
    }

    public function testGetTripleCountWithEmptyGraph(): void
    {
        $graph = new ExtendedGraph();
        $expected = 0;
        $actual = $graph->get_triple_count();
        $this->assertSame($expected, $actual);
    }

    public function testGetTripleCountWithNonExistentSubject(): void
    {
        $graph = new ExtendedGraph();
        $expected = 0;
        $actual = $graph->get_triple_count('http://example.com/subject');
        $this->assertSame($expected, $actual);
    }

    public function testReplaceUris(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/4', 'http://some/predicate', 'http://some/subject/1');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://some/subject/1');
        $graph->replace_uris('http://some/subject/1', 'http://some/subject/3');

        $index = $graph->get_index();
        $this->assertFalse($graph->has_triples_about('http://some/subject/1'), 'resource with old uri still exists');
        $this->assertTrue($graph->has_triples_about('http://some/subject/3'), "resource with new uri doesn't exists");
        $this->assertEquals('http://some/subject/3', $index['http://some/subject/2']['http://some/predicate'][0]['value'], 'http://some/subject/3');
        $this->assertTrue($graph->has_literal_triple('http://some/subject/4', 'http://some/predicate', 'http://some/subject/1'), 'literal values should not be replaced');

        $graph->replace_uris('http://some/predicate', 'http://some/predicate2');
        $index = $graph->get_index();
        $this->assertArrayHasKey('http://some/predicate2', $index['http://some/subject/2'], 'predicate should be replaced');
        $this->assertArrayNotHasKey('http://some/predicate', $index['http://some/subject/2'], 'predicate should be replaced and old one not be in graph');
    }

    public function testReplaceResourceTriples(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://some/subject/1');
        $graph->replace_resource_triples('http://some/subject/2', 'http://some/predicate', 'http://some/subject/3');

        $index = $graph->get_index();
        $this->assertEquals('http://some/subject/3', $index['http://some/subject/2']['http://some/predicate'][0]['value'], 'http://some/subject/3');
    }

    public function testReplaceLiteralTriple(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://some/subject/s1', 'http://some/predicate', 'some object');
        $this->assertTrue($graph->replace_literal_triple('http://some/subject/s1', 'http://some/predicate', 'some object', 'replacement object'), 'Should return true on replacement');
        $index = $graph->get_index();
        $this->assertEquals('replacement object', $index['http://some/subject/s1']['http://some/predicate'][0]['value'], "should be 'replacement object'");
    }

    public function testReplaceLiteralTripleReturnsFalseIfNoReplacementMade(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://some/subject/s1', 'http://some/predicate', 'some object');
        $this->assertFalse($graph->replace_literal_triple('http://some/othersubject/s1', 'http://some/predicate', 'some object', 'replacement object'), 'Should return FALSE');
    }

    public function testGetResources(): void
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate/p1', 'some object');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate/p2', 'http://some/object/1');
        $graph->add_resource_triple('http://some/subject/3', 'http://some/predicate/p3', 'http://some/object/2');
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate/p3', 'http://this/is/not/a/rsource');

        $expected = ['http://some/subject/1', 'http://some/subject/2', 'http://some/subject/3', 'http://some/object/1', 'http://some/object/2'];

        $actual = $graph->get_resources();

        sort($expected);
        sort($actual);

        $this->assertCount(count($expected), $actual, 'should get same number of resource uris');
        $this->assertEquals($expected, $actual, 'should get expected array containing all the resource uris');
    }

    public function testGetLabelForUri(): void
    {
        $graph = new ExtendedGraph();

        $label = 'Wuthering Heights';
        $s1 = 'http://example.com/resource';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph->add_resource_triple($s1, $rdfType, 'http://xmlns.com/foaf/0.1/Person');
        $graph->add_literal_triple($s1, 'http://www.w3.org/2000/01/rdf-schema#label', $label);

        ExtendedGraph::initProperties(['labelProperties' => ['http://www.w3.org/2000/01/rdf-schema#label']]);

        $this->assertSame($graph->get_label_for_uri($s1), $label, 'get_label_for_uri(uri) should return the value of a label property');
    }

    public function testGetLabelForUriLabelPropsNotInitialised(): void
    {
        ExtendedGraph::initProperties(['labelProperties' => null]);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Please initialise ExtendedGraph::$labelProperties');
        $graph = new ExtendedGraph();

        $label = 'Wuthering Heights';
        $s1 = 'http://example.com/resource';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph->add_resource_triple($s1, $rdfType, 'http://xmlns.com/foaf/0.1/Person');
        $graph->add_literal_triple($s1, 'http://www.w3.org/2000/01/rdf-schema#label', $label);

        $graph->get_label_for_uri($s1);
    }

    public function testGetLabelForUriReturnsEmptyStringIfSubjectNotFound(): void
    {
        $graph = new ExtendedGraph();

        $label = 'Wuthering Heights';
        $s1 = 'http://example.com/resource';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph->add_resource_triple($s1, $rdfType, 'http://xmlns.com/foaf/0.1/Person');
        $graph->add_literal_triple($s1, 'http://www.w3.org/2000/01/rdf-schema#label', $label);

        ExtendedGraph::initProperties(['labelProperties' => ['http://www.w3.org/2000/01/rdf-schema#label']]);

        $this->assertSame('', $graph->get_label_for_uri('http://example.com/1'));
    }

    public function testGetLabelForUriReturnsEmptyStringLabelNotFound(): void
    {
        $graph = new ExtendedGraph();

        $label = 'Wuthering Heights';
        $s1 = 'http://example.com/resource';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph->add_resource_triple($s1, $rdfType, 'http://xmlns.com/foaf/0.1/Person');
        $graph->add_literal_triple($s1, 'http://www.w3.org/2000/01/rdf-schema#label', $label);

        ExtendedGraph::initProperties(['labelProperties' => ['http://www.w3.org/2000/01/rdf-schema#label2']]);

        $this->assertSame('', $graph->get_label_for_uri($s1));
    }

    public function testIsEqualToReturnsTrueForIdenticalGraphs(): void
    {
        $s = 'http://example.com/people/bloggs-joe';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph1 = new ExtendedGraph();
        $graph1->add_resource_triple($s, $rdfType, 'http://xmlns.com/foaf/0.1/Person');

        $graph2 = new ExtendedGraph();
        $graph2->add_resource_triple($s, $rdfType, 'http://xmlns.com/foaf/0.1/Person');

        $this->assertTrue($graph1->is_equal_to($graph2), 'graph1 should equal graph2');
        $this->assertTrue($graph2->is_equal_to($graph1), 'graph2 should equal graph1');
    }

    public function testIsEqualToReturnsFalseForDifferingGraphs(): void
    {
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph1 = new ExtendedGraph();
        $graph1->add_resource_triple('http://example.com/people/bloggs-joe', $rdfType, 'http://xmlns.com/foaf/0.1/Person');

        $graph2 = new ExtendedGraph();
        $graph2->add_resource_triple('http://example.com/people/mouse-mickey', $rdfType, 'http://xmlns.com/foaf/0.1/Person');

        $graph3 = new ExtendedGraph();
        $graph3->add_literal_triple('http://example.com/people/mouse-mickey', $rdfType, 'http://xmlns.com/foaf/0.1/Person', 'en');

        $graph4 = new ExtendedGraph();
        $graph4->add_resource_triple('http://example.com/people/mouse-mickey', 'http://some/predicate/p1', 'http://xmlns.com/foaf/0.1/Person');

        $graph5 = new ExtendedGraph();
        $graph5->add_resource_triple('http://example.com/people/mouse-mickey', $rdfType, 'http://xmlns.com/foaf/0.1/Mouse');

        $graph6 = new ExtendedGraph();
        $graph6->add_resource_triple('http://example.com/people/mouse-mickey', $rdfType, 'http://xmlns.com/foaf/0.1/Person');

        // Different subject
        $this->assertFalse($graph1->is_equal_to($graph2), 'graph1 should not equal graph2');
        $this->assertFalse($graph2->is_equal_to($graph1), 'graph2 should not equal graph1');

        // Different type
        $this->assertFalse($graph2->is_equal_to($graph3), 'graph2 should not equal graph3');
        $this->assertFalse($graph3->is_equal_to($graph2), 'graph3 should not equal graph2');

        // Different predicate
        $this->assertFalse($graph2->is_equal_to($graph4), 'graph2 should not equal graph4');
        $this->assertFalse($graph4->is_equal_to($graph2), 'graph4 should not equal graph2');

        // Different object
        $this->assertFalse($graph2->is_equal_to($graph5), 'graph2 should not equal graph5');
        $this->assertFalse($graph5->is_equal_to($graph2), 'graph5 should not equal graph2');
    }

    public function testIsEqualToIgnoresNamespaceDifferences(): void
    {
        $s = 'http://example.com/people/bloggs-joe';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph1 = new ExtendedGraph();
        $graph1->add_resource_triple($s, $rdfType, 'http://xmlns.com/foaf/0.1/Person');

        $graph2 = new ExtendedGraph();
        $graph2->set_namespace_mapping('foaf', self::ONT_foaf);
        $graph2->add_resource_triple($s, $rdfType, 'http://xmlns.com/foaf/0.1/Person');

        $graph3 = new ExtendedGraph();
        $graph3->set_namespace_mapping('FAF', self::ONT_foaf);
        $graph3->add_resource_triple($s, $rdfType, 'http://xmlns.com/foaf/0.1/Person');

        $this->assertTrue($graph1->is_equal_to($graph2), 'graph1 should equal graph2');
        $this->assertTrue($graph2->is_equal_to($graph1), 'graph2 should equal graph1');

        $this->assertTrue($graph2->is_equal_to($graph3), 'graph2 should equal graph3');
        $this->assertTrue($graph3->is_equal_to($graph2), 'graph3 should equal graph2');
    }

    public function testRemoveResourceFromSequence(): void
    {
        $graph = new ExtendedGraph();

        $s = 'http://sequence';
        $sub1 = 'http://sub1';
        $sub2 = 'http://sub2';
        $sub3 = 'http://sub3';

        $graph->add_resource_to_sequence($s, $sub1);
        $graph->add_resource_to_sequence($s, $sub2);
        $graph->add_resource_to_sequence($s, $sub3);

        $graph->remove_resource_from_sequence($s, $sub2);

        $sequenceValues = $graph->get_sequence_values($s);
        $this->assertSame([$sub1, $sub3], $sequenceValues, 'There should be two sequence values, in the correct order');
        $this->assertTrue($graph->has_resource_triple($s, ExtendedGraph::rdf . '_1', $sub1));
        $this->assertTrue($graph->has_resource_triple($s, ExtendedGraph::rdf . '_2', $sub3));
    }

    public function testFromGraph(): void
    {
        $itemUri = 'http://foo/item';
        $mainResourceUri = 'http://foo/mainResource';

        $itemGraph = new ExtendedGraph();

        $itemGraph->add_resource_triple($itemUri, ExtendedGraph::rdf . 'type', self::ONT_resource . 'Item');
        $itemGraph->add_resource_triple($itemUri, self::ONT_resource . 'resource', $mainResourceUri);

        $graph = new ExtendedGraph();
        $graph->from_graph($itemGraph);

        $this->assertTrue($graph->is_equal_to($itemGraph));
    }

    public function testRemoveSubjectsOfType(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://test/1', ExtendedGraph::rdf . 'type', self::ONT_resource . 'Item');
        $graph->add_resource_triple('http://test/2', ExtendedGraph::rdf . 'type', self::ONT_resource . 'Item');
        $graph->add_resource_triple('http://test/3', ExtendedGraph::rdf . 'type', self::ONT_resource . 'List');

        $graph->remove_subjects_of_type(self::ONT_resource . 'Item');

        $subjects = $graph->get_subjects();
        $this->assertCount(1, $subjects);
        $this->assertEquals('http://test/3', $subjects[0]);
    }

    public function testReplaceLiteralTriples(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://test/1', 'http://www.w3.org/2000/01/rdf-schema#label', 'value1');
        $graph->add_literal_triple('http://test/1', 'http://www.w3.org/2000/01/rdf-schema#label', 'value2');

        $graph->replace_literal_triples('http://test/1', 'http://www.w3.org/2000/01/rdf-schema#label', 'value3');
        $this->assertTrue($graph->has_literal_triple('http://test/1', 'http://www.w3.org/2000/01/rdf-schema#label', 'value3'));
        $this->assertFalse($graph->has_literal_triple('http://test/1', 'http://www.w3.org/2000/01/rdf-schema#label', 'value1'));
        $this->assertFalse($graph->has_literal_triple('http://test/1', 'http://www.w3.org/2000/01/rdf-schema#label', 'value2'));
    }

    public function testFromJson(): void
    {
        $graph = new ExtendedGraph();
        $graph->from_json('{
            "http://subject/1": {
                "http://predicate": [
                    { "type": "uri", "value": "http://value/1" }
                ]
            },
            "http://subject/2": {
                "http://predicate": [
                    { "type": "uri", "value": "http://value/2" }
                ]
            },
            "http://subject/3": {
                "http://predicate": [
                    { "type": "uri", "value": "http://value/3" }
                ]
            }
        }');

        $this->assertCount(3, $graph->get_subjects());
        $this->assertSame(['http://subject/1', 'http://subject/2', 'http://subject/3'], $graph->get_subjects());
        $this->assertSame(['http://value/1', 'http://value/2', 'http://value/3'], $graph->get_resource_properties('http://predicate'));
    }

    public function testFromInvalidJson(): void
    {
        $graph = new ExtendedGraph();
        $index = $graph->get_index();

        $graph->from_json('not a valid json');

        // Should not have changed
        $this->assertEquals($index, $graph->get_index());
    }

    public function testAddJson(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_json('{
            "http://subject/1": {
                "http://predicate": [
                    { "type": "uri", "value": "http://value/1" }
                ]
            }
        }');

        $this->assertCount(1, $graph->get_subjects());
        $this->assertSame(['http://subject/1'], $graph->get_subjects());
        $this->assertSame(['http://value/1'], $graph->get_resource_properties('http://predicate'));

        $graph->add_json('{
            "http://subject/2": {
                "http://predicate": [
                    { "type": "uri", "value": "http://value/2" }
                ]
            }
        }');

        $this->assertCount(2, $graph->get_subjects());
        $this->assertSame(['http://subject/1', 'http://subject/2'], $graph->get_subjects());
        $this->assertSame(['http://value/1', 'http://value/2'], $graph->get_resource_properties('http://predicate'));
    }

    public function testAddInvalidJson(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_json('{
            "http://subject/1": {
                "http://predicate": [
                    { "type": "uri", "value": "http://value/1" }
                ]
            }
        }');

        $this->assertSame(1, $graph->get_triple_count());
        $this->assertCount(1, $graph->get_subjects());
        $this->assertSame(['http://subject/1'], $graph->get_subjects());
        $this->assertSame(['http://value/1'], $graph->get_resource_properties('http://predicate'));
        $index = $graph->get_index();

        $graph->add_json('not a valid json');

        // Should not have changed
        $this->assertEquals($index, $graph->get_index());
        $this->assertSame(1, $graph->get_triple_count());
    }

    public function testMakeResourceArray(): void
    {
        $graph = new ExtendedGraph();
        $this->assertSame(['type' => 'uri', 'value' => 'http://example.com/1'], $graph->make_resource_array('http://example.com/1'));
        $this->assertSame(['type' => 'bnode', 'value' => '_:b1'], $graph->make_resource_array('_:b1'));
    }

    public function testGetPrefixAndAddLabellingPropertyDelegateToLabeller(): void
    {
        $graph = new ExtendedGraph();
        $this->assertSame('dct', $graph->get_prefix('http://purl.org/dc/terms/'));
        // add_labelling_property makes the property considered by get_label
        $graph->add_labelling_property('http://example.com/vocab#customLabel');
        $graph->add_literal_triple('http://example.com/1', 'http://example.com/vocab#customLabel', 'Custom', 'en');
        $this->assertSame('Custom', $graph->get_label('http://example.com/1'));
    }

    public function testGetLabelAndInverseLabelDelegateToLabeller(): void
    {
        $graph = new ExtendedGraph();
        $this->assertSame('date created', $graph->get_label('http://purl.org/dc/terms/created'));
        $this->assertSame('is date created of', $graph->get_inverse_label('http://purl.org/dc/terms/created'));
    }

    public function testGetTriples(): void
    {
        $triples = $this->getSimpleGraph()->get_triples();

        $this->assertCount(2, $triples);
        $literals = array_values(array_filter($triples, function ($t) {
            return $t['o_type'] === 'literal';
        }));
        $this->assertSame('A Title', $literals[0]['o']);
        $this->assertSame('http://example.com/1', $literals[0]['s']);
        $this->assertSame('http://purl.org/dc/terms/title', $literals[0]['p']);
    }

    public function testToRdfXmlAndFromRdfXmlRoundTrip(): void
    {
        $graph = $this->getSimpleGraph();
        $rdfxml = $graph->to_rdfxml();
        $this->assertStringContainsString('A Title', $rdfxml);
        $this->assertStringContainsString('http://example.com/1', $rdfxml);

        $newGraph = new ExtendedGraph();
        $newGraph->add_literal_triple('http://example.com/other', 'http://purl.org/dc/terms/title', 'Removed on parse');
        $newGraph->from_rdfxml($rdfxml);
        $this->assertEquals($graph->get_index(), $newGraph->get_index());
        $this->assertFalse($newGraph->has_triples_about('http://example.com/other'));
    }

    public function testFromRdfXmlWithEmptyStringLeavesGraphUnchanged(): void
    {
        $graph = $this->getSimpleGraph();
        $index = $graph->get_index();
        $graph->from_rdfxml('');
        $this->assertEquals($index, $graph->get_index());
    }

    public function testToTurtleAndFromTurtleRoundTrip(): void
    {
        $graph = $this->getSimpleGraph();
        $turtle = $graph->to_turtle();
        $this->assertStringContainsString('A Title', $turtle);

        $newGraph = new ExtendedGraph();
        $newGraph->from_turtle($turtle);
        $this->assertEquals($graph->get_index(), $newGraph->get_index());
    }

    public function testToJsonAndFromJsonRoundTrip(): void
    {
        $graph = $this->getSimpleGraph();
        $json = $graph->to_json();
        $this->assertSame($graph->get_index(), json_decode($json, true));

        $newGraph = new ExtendedGraph();
        $newGraph->from_json($json);
        $this->assertEquals($graph->get_index(), $newGraph->get_index());
    }

    public function testFromRdfaParsesRdfaMarkup(): void
    {
        $html = '<html xmlns="http://www.w3.org/1999/xhtml"><head><title>test</title></head><body>'
            . '<div about="http://example.com/1"><span property="http://purl.org/dc/terms/title">A Title</span></div>'
            . '</body></html>';

        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/other', 'http://purl.org/dc/terms/title', 'Removed on parse');
        $graph->from_rdfa($html);

        $this->assertFalse($graph->has_triples_about('http://example.com/other'));
        $this->assertSame('A Title', $graph->get_first_literal('http://example.com/1', 'http://purl.org/dc/terms/title'));
    }

    public function testToHtml(): void
    {
        $graph = $this->getSimpleGraph();
        $graph->add_literal_triple('http://example.com/2', 'http://purl.org/dc/terms/title', 'Second Title');

        $html = $graph->to_html();
        $this->assertStringContainsString('A Title', $html);
        $this->assertStringContainsString('Second Title', $html);

        // restricted to a single subject
        $html = $graph->to_html('http://example.com/1');
        $this->assertStringContainsString('A Title', $html);
        $this->assertStringNotContainsString('Second Title', $html);

        // restricted to an array of subjects
        $html = $graph->to_html(['http://example.com/2']);
        $this->assertStringContainsString('Second Title', $html);

        // unknown subjects produce no output
        $this->assertSame('', $graph->to_html('http://example.com/unknown'));
        $this->assertSame('', $graph->to_html(['http://example.com/unknown']));
    }

    public function testAddRdfCollectsParserErrors(): void
    {
        $graph = new ExtendedGraph();
        $this->assertSame([], $graph->get_parser_errors());
        $graph->add_rdf('<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><rdf:unclosed</rdf:RDF>');
        $this->assertNotEmpty($graph->get_parser_errors());
    }

    public function testGetLiteralTripleValues(): void
    {
        $graph = $this->getSimpleGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/description', 'A Description');

        // resource triples with the same subject are excluded
        $this->assertSame(['A Title'], $graph->get_literal_triple_values('http://example.com/1', 'http://purl.org/dc/terms/title'));
        $this->assertSame([], $graph->get_literal_triple_values('http://example.com/1', 'http://purl.org/dc/terms/source'));

        // array of predicates
        $this->assertSame(
            ['A Title', 'A Description'],
            $graph->get_literal_triple_values('http://example.com/1', ['http://purl.org/dc/terms/title', 'http://purl.org/dc/terms/description'])
        );

        // unknown subject
        $this->assertSame([], $graph->get_literal_triple_values('http://example.com/unknown', 'http://purl.org/dc/terms/title'));
    }

    public function testGetSubjectsWhereLiteral(): void
    {
        $graph = $this->getSimpleGraph();
        $graph->add_literal_triple('http://example.com/2', 'http://purl.org/dc/terms/title', 'A Title');
        $graph->add_resource_triple('http://example.com/3', 'http://purl.org/dc/terms/title', 'http://example.com/A_Title');

        $subjects = $graph->get_subjects_where_literal('http://purl.org/dc/terms/title', 'A Title');
        sort($subjects);
        $this->assertSame(['http://example.com/1', 'http://example.com/2'], $subjects);
        $this->assertSame([], $graph->get_subjects_where_literal('http://purl.org/dc/terms/title', 'No Such Title'));
    }

    public function testReplaceResourceInSubjectPosition(): void
    {
        $graph = $this->getSimpleGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/description', 'A Description', 'en');

        $graph->replace_resource('http://example.com/1', 'http://example.com/new');

        $this->assertFalse($graph->has_triples_about('http://example.com/1'));
        $this->assertSame('A Title', $graph->get_first_literal('http://example.com/new', 'http://purl.org/dc/terms/title'));
        $this->assertSame('http://example.com/source', $graph->get_first_resource('http://example.com/new', 'http://purl.org/dc/terms/source'));

        // language on literals is preserved
        $index = $graph->get_index();
        $this->assertSame('en', $index['http://example.com/new']['http://purl.org/dc/terms/description'][0]['lang']);
    }

    public function testReplaceResourceInObjectPosition(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://example.com/1', 'http://purl.org/dc/terms/source', 'http://example.com/old');

        $graph->replace_resource('http://example.com/old', 'http://example.com/new');

        $this->assertSame('http://example.com/new', $graph->get_first_resource('http://example.com/1', 'http://purl.org/dc/terms/source'));
    }

    public function testGetListValues(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://example.com/list', RDF_FIRST, 'http://example.com/item/1');
        $graph->add_resource_triple('http://example.com/list', RDF_REST, '_:rest1');
        $graph->add_resource_triple('_:rest1', RDF_FIRST, 'http://example.com/item/2');
        $graph->add_resource_triple('_:rest1', RDF_REST, RDF_NIL);

        $this->assertSame(
            ['http://example.com/item/1', 'http://example.com/item/2'],
            $graph->get_list_values('http://example.com/list')
        );
    }

    public function testConstructorAcceptsIndexArray(): void
    {
        $index = [
            'http://example.com/1' => [
                'http://purl.org/dc/terms/title' => [['type' => 'literal', 'value' => 'A Title']],
            ],
        ];
        $graph = new ExtendedGraph($index);
        $this->assertSame($index, $graph->get_index());
    }

    public function testConstructorAcceptsRdfString(): void
    {
        $graph = new ExtendedGraph('{"http://example.com/1":{"http://purl.org/dc/terms/title":[{"type":"literal","value":"A Title"}]}}');
        $this->assertSame('A Title', $graph->get_first_literal('http://example.com/1', 'http://purl.org/dc/terms/title'));
    }

    public function testAddLiteralTripleWithDatatype(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/extent', '123', null, 'http://www.w3.org/2001/XMLSchema#integer');

        $index = $graph->get_index();
        $this->assertSame('http://www.w3.org/2001/XMLSchema#integer', $index['http://example.com/1']['http://purl.org/dc/terms/extent'][0]['datatype']);
        $this->assertTrue($graph->has_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/extent', '123', null, 'http://www.w3.org/2001/XMLSchema#integer'));
        $this->assertFalse($graph->has_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/extent', '123', null, 'http://www.w3.org/2001/XMLSchema#string'));
    }

    public function testHasLiteralTripleWithLanguage(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'Bonjour', 'fr');
        $this->assertTrue($graph->has_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'Bonjour', 'fr'));
        $this->assertFalse($graph->has_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'Bonjour', 'en'));
    }

    public function testGetFirstLiteralWithArrayOfPredicatesAndPreferredLanguage(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'Bonjour', 'fr');
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'Hello', 'en');

        $predicates = ['http://purl.org/dc/terms/alternative', 'http://purl.org/dc/terms/title'];
        $this->assertSame('Hello', $graph->get_first_literal('http://example.com/1', $predicates, null, 'en'));
        // no literal with the preferred language falls back to the last literal found
        $this->assertSame('Hello', $graph->get_first_literal('http://example.com/1', $predicates, null, 'de'));
        // no preferred language specified returns the first literal found
        $this->assertSame('Bonjour', $graph->get_first_literal('http://example.com/1', $predicates, null, null));
        // no matching predicates returns the default
        $this->assertSame('default', $graph->get_first_literal('http://example.com/1', ['http://purl.org/dc/terms/description'], 'default'));
    }

    public function testAddRdfWithJsonInput(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_rdf('{"http://example.com/1":{"http://purl.org/dc/terms/title":[{"type":"literal","value":"From JSON"}]}}');
        $this->assertSame('From JSON', $graph->get_first_literal('http://example.com/1', 'http://purl.org/dc/terms/title'));
    }

    public function testGetSubjectPropertiesNonDistinct(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'One');
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'Two');

        $this->assertSame(['http://purl.org/dc/terms/title'], $graph->get_subject_properties('http://example.com/1'));
        $this->assertSame(
            ['http://purl.org/dc/terms/title', 'http://purl.org/dc/terms/title'],
            $graph->get_subject_properties('http://example.com/1', false)
        );
    }

    public function testDiffAgainstOwnIndex(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'Kept');
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/description', 'Removed');

        $other = new ExtendedGraph();
        $other->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'Kept');

        // single argument diffs the graph's own index against the supplied one
        $diff = $graph->diff($other->get_index());
        $this->assertArrayHasKey('http://purl.org/dc/terms/description', $diff['http://example.com/1']);
        $this->assertArrayNotHasKey('http://purl.org/dc/terms/title', $diff['http://example.com/1']);
    }

    public function testMergeRenamesClashingBnodes(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('_:a', 'http://purl.org/dc/terms/title', 'Original bnode');

        $incoming = [
            '_:a' => [
                'http://purl.org/dc/terms/title' => [['type' => 'literal', 'value' => 'Incoming bnode']],
            ],
            'http://example.com/1' => [
                'http://purl.org/dc/terms/source' => [['type' => 'bnode', 'value' => '_:a']],
            ],
        ];

        $merged = $graph->merge($incoming);

        // the incoming bnode is renamed to avoid clashing with the existing one
        $this->assertSame('Original bnode', $merged['_:a']['http://purl.org/dc/terms/title'][0]['value']);
        $this->assertSame('Incoming bnode', $merged['_:a1']['http://purl.org/dc/terms/title'][0]['value']);
        // references to the renamed bnode are rewritten
        $this->assertSame('_:a1', $merged['http://example.com/1']['http://purl.org/dc/terms/source'][0]['value']);
    }

    public function testReplaceResourceInPredicatePosition(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://example.com/oldPredicate', 'A value');
        $graph->add_resource_triple('http://example.com/1', 'http://example.com/oldPredicate', 'http://example.com/other');

        $graph->replace_resource('http://example.com/oldPredicate', 'http://example.com/newPredicate');

        $this->assertFalse($graph->subject_has_property('http://example.com/1', 'http://example.com/oldPredicate'));
        $this->assertSame(['A value'], $graph->get_literal_triple_values('http://example.com/1', 'http://example.com/newPredicate'));
        $this->assertSame(['http://example.com/other'], $graph->get_resource_triple_values('http://example.com/1', 'http://example.com/newPredicate'));
    }

    public function testReplaceResourceWhenUriIsSubjectPredicateAndObject(): void
    {
        $graph = new ExtendedGraph();
        $uri = 'http://example.com/self';
        $graph->add_resource_triple($uri, $uri, $uri);
        $graph->add_literal_triple($uri, $uri, 'self literal');

        $graph->replace_resource($uri, 'http://example.com/new');

        $this->assertFalse($graph->has_triples_about($uri));
        $this->assertTrue($graph->has_resource_triple('http://example.com/new', 'http://example.com/new', 'http://example.com/new'));
        $this->assertTrue($graph->has_literal_triple('http://example.com/new', 'http://example.com/new', 'self literal'));
    }

    public function testToHtmlRendersPropertiesAndBacklinks(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'First Title');
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'Second Title');
        $graph->add_resource_triple('http://example.com/1', 'http://purl.org/dc/terms/source', 'http://example.com/2');
        $graph->add_resource_triple('http://example.com/3', 'http://purl.org/dc/terms/source', 'http://example.com/2');
        $graph->add_literal_triple('http://example.com/2', 'http://purl.org/dc/terms/title', 'Linked Title');

        $html = $graph->to_html('http://example.com/2');
        // the subject linking here is rendered as a backlink with an inverse label
        $this->assertStringContainsString('http://example.com/1', $html);
        $this->assertStringContainsString('Linked Title', $html);

        // multiple values for the same property are separated
        $html = $graph->to_html('http://example.com/1');
        $this->assertStringContainsString('First Title', $html);
        $this->assertStringContainsString('Second Title', $html);
        $this->assertStringContainsString('<br />', $html);

        // guess_labels=false renders raw URIs instead of labels
        $html = $graph->to_html(null, false);
        $this->assertStringContainsString('>http://example.com/2</a>', $html);
        $this->assertStringContainsString('>http://example.com/3</a>', $html);
    }

    public function testAddTurtleWithBnodesIntoNonEmptyGraphRenamesBnodes(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/existing', 'http://purl.org/dc/terms/title', 'Existing');

        $turtle = '@prefix dct: <http://purl.org/dc/terms/> .
<http://example.com/1> dct:contributor _:b1 .
_:b1 dct:title "A bnode title"@en .
<http://example.com/1> dct:extent "123"^^<http://www.w3.org/2001/XMLSchema#integer> .';

        $graph->add_turtle($turtle);

        // bnode labels are rewritten to avoid clashes with the non-empty graph
        $bnode = $graph->get_first_resource('http://example.com/1', 'http://purl.org/dc/terms/contributor');
        $this->assertStringStartsWith('_:mor', $bnode);
        $this->assertSame('A bnode title', $graph->get_first_literal($bnode, 'http://purl.org/dc/terms/title'));

        // language and datatype survive parsing
        $index = $graph->get_index();
        $this->assertSame('en', $index[$bnode]['http://purl.org/dc/terms/title'][0]['lang']);
        $this->assertSame(
            'http://www.w3.org/2001/XMLSchema#integer',
            $index['http://example.com/1']['http://purl.org/dc/terms/extent'][0]['datatype']
        );
    }

    private function getSimpleGraph(): ExtendedGraph
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/1', 'http://purl.org/dc/terms/title', 'A Title');
        $graph->add_resource_triple('http://example.com/1', 'http://purl.org/dc/terms/source', 'http://example.com/source');

        return $graph;
    }
}
