<?php
set_include_path(
    get_include_path()
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__)))
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/lib'
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src');

require_once('tripod.inc.php');
require_once 'src/classes/ExtendedGraph.class.php';
require_once 'src/exceptions/Exception.class.php';

use \Tripod\ExtendedGraph;

class ExtendedGraphTest extends PHPUnit_Framework_TestCase
{
    const ONT_foaf = 'http://xmlns.com/foaf/0.1/';
    const ONT_resource = 'http://purl.org/vocab/resourcelist/schema#';

    protected function setUp()
    {
        $className = get_class($this);
        $testName = $this->getName();
        echo "\nTest: {$className}->{$testName}\n";
    }

    /**
     * @dataProvider addValidValueToLiteralResultsInTriple_Provider
     */
    public function testAddValidValueToLiteralResultsInTriple($value)
    {
        $graph = new ExtendedGraph();
        $addResult = $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', $value);
        $this->assertTrue($addResult, 'The triple should have been added for this value');

        $hasPropertyResult = $graph->subject_has_property('http://some/subject/1', 'http://some/predicate');
        $this->assertTrue($hasPropertyResult, 'The triple should have been added for this value');
    }
    public function addValidValueToLiteralResultsInTriple_Provider(){
        return array(
            array('String'),
            array(1),
            array(1.2),
            array(true)
        );
    }

    /**
     * @dataProvider addInvalidValueToLiteralResultsInNoTriple_Provider
     */
    public function testAddInvalidValueToLiteralResultsInNoTriple($value)
    {
        $graph = new ExtendedGraph();
        $addResult = $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', $value);
        $this->assertFalse($addResult, 'The triple should not have been added for this value');

        $hasPropertyResult = $graph->subject_has_property('http://some/subject/1', 'http://some/predicate');
        $this->assertFalse($hasPropertyResult, 'The triple should not have been added for this value');
    }
    public function addInvalidValueToLiteralResultsInNoTriple_Provider(){
        return array(
            array(null),
            array(new stdClass()),
            array(function(){})
        );
    }

    /**
     * @dataProvider addInvalidSubjectToLiteralResultsInNoTriple_Provider
     */
    public function testAddInvalidSubjectToLiteralThrowsException($value)
    {
        $this->setExpectedException('\Tripod\Exceptions\Exception');

        $graph = new ExtendedGraph();
        $graph->add_resource_triple($value, 'http://some/predicate', 'http://someplace.com');
    }
    public function addInvalidSubjectToLiteralResultsInNoTriple_Provider(){
        return array(
            array(""),
            array(1),
            array(1.2),
            array(true),
            array(array()),
            array(null),
            array(new stdClass()),
            array(function(){})
        );
    }

    /**
     * @dataProvider addInvalidSubjectToLiteralResultsInNoTriple_Provider
     */
    public function testAddInvalidPredicateToLiteralThrowsException($value)
    {
        $this->setExpectedException('\Tripod\Exceptions\Exception');

        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://some/subject/1', $value, 'http://someplace.com');
    }
    public function addInvalidPredicateToLiteralResultsInNoTriple_Provider(){
        return array(
            array(""),
            array(1),
            array(1.2),
            array(true),
            array(array()),
            array(null),
            array(new stdClass()),
            array(function(){})
        );
    }

    public function testAddValidValueToResourceResultsInTriple()
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
     */
    public function testAddInvalidValueToResourceResultsInNoTriple($value)
    {
        $graph = new ExtendedGraph();

        $addResult = $graph->add_resource_triple('http://some/subject/1', 'http://some/predicate', $value);
        $this->assertFalse($addResult, 'The triple should not have been added for this value');

        $hasPropertyResult = $graph->subject_has_property('http://some/subject/1', 'http://some/predicate');
        $this->assertFalse($hasPropertyResult, 'The triple should not have been added for this value');
    }
    public function addInvalidValueToResourceResultsInNoTriple_Provider(){
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

    /**
     * @dataProvider addInvalidSubjectToResourceResultsInNoTriple_Provider
     */
    public function testAddInvalidSubjectToResourceThrowsException($value)
    {
        $this->setExpectedException('\Tripod\Exceptions\Exception');

        $graph = new ExtendedGraph();
        $graph->add_resource_triple($value, 'http://some/predicate', 'http://someplace.com');
    }
    public function addInvalidSubjectToResourceResultsInNoTriple_Provider(){
        return array(
            array(""),
            array(1),
            array(1.2),
            array(true),
            array(array()),
            array(null),
            array(new stdClass()),
            array(function(){})
        );
    }

    /**
     * @dataProvider addInvalidSubjectToLiteralResultsInNoTriple_Provider
     */
    public function testAddInvalidPredicateToResourceThrowsException($value)
    {
        $this->setExpectedException('\Tripod\Exceptions\Exception');

        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://some/subject/1', $value, 'http://someplace.com');
    }
    public function addInvalidPredicateToResourceResultsInNoTriple_Provider(){
        return array(
            array(""),
            array(1),
            array(1.2),
            array(true),
            array(array()),
            array(null),
            array(new stdClass()),
            array(function(){})
        );
    }

    public function testRemoveProperties()
    {
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate/to/remove', 'some object');
        $graph->add_resource_triple('http://some/subject/3', 'http://some/predicate/to/remove', 'http://some/other/object');
        $graph->add_literal_triple('http://some/subject/4', 'http://some/predicate', 'some object');

        $graph->remove_properties('http://some/predicate/to/remove');

        $this->assertFalse($graph->subject_has_property('http://some/subject/2','http://some/predicate/to/remove'), 'should have removed triple about subject 2');
        $this->assertFalse($graph->subject_has_property('http://some/subject/3','http://some/predicate/to/remove'), 'should have removed triple about subject 3');
    }

    public function testGetResourceProperties()
    {
        $graph = new ExtendedGraph();

        $graph->add_resource_triple('http://some/subject/1', 'http://some/predicate', 'http://value/1');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://value/2');
        $graph->add_resource_triple('http://some/subject/3', 'http://some/predicate', 'http://value/3');

        $values = $graph->get_resource_properties('http://some/predicate');

        $this->assertEquals($values, array('http://value/1', 'http://value/2', 'http://value/3'), 'should have returned 3 values');
    }

    public function testGetSubjectsWithPropertyValue()
    {
        $graph = new ExtendedGraph();

        $graph->add_resource_triple('http://some/subject/1', 'http://some/predicate', 'http://value/1');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://value/2');
        $graph->add_resource_triple('http://some/subject/3', 'http://some/predicate', 'http://value/3');

        $graph->add_literal_triple('http://some/subject/2-with-literal', 'http://some/predicate', 'http://value/2');

        $subjects = $graph->get_subjects_with_property_value('http://some/predicate', 'http://value/2');

        $this->assertEquals($subjects, array('http://some/subject/2', 'http://some/subject/2-with-literal'), 'should have returned correct subject');
    }

    public function testGetSequenceValues()
    {
        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_4', 'http://value/4');
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_2', 'http://value/2');
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_3', 'http://value/3');
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_5', 'http://value/5');
        $graph->add_resource_triple('http://some/subject/1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_1', 'http://value/1');


        $expectedArray = array('http://value/1', 'http://value/2', 'http://value/3', 'http://value/4', 'http://value/5');
        $this->assertEquals($expectedArray, $graph->get_sequence_values('http://some/subject/1'));
    }

    public function testAddResourceToSequence()
    {
        $testSubject = 'http://some/subject/s1';
        $testObject1 = 'http://some/object/o1';
        $testObject2 = 'http://example.com/foo/bar';

        $graph = new ExtendedGraph();
        $graph->add_resource_to_sequence($testSubject, $testObject1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject1), $objects);

        $graph->add_resource_to_sequence($testSubject, $testObject2);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject1, $testObject2), $objects);

        $graph->add_resource_to_sequence('http://some/other/subject', 'http://some/other/object');

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject1, $testObject2), $objects);

        $objects = $graph->get_sequence_values('http://some/other/subject');
        $this->assertEquals(array('http://some/other/object'), $objects);
    }

    public function testAddResourceToSequenceInPosition()
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
        $this->assertEquals(array($testObject1), $objects);

        $graph->add_resource_to_sequence_in_position($testSubject, $testObject2, 1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject2, $testObject1), $objects);

        $graph->add_resource_to_sequence_in_position($testSubject, $testObject3, 1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject3, $testObject2, $testObject1), $objects);

        $graph->add_resource_to_sequence_in_position($testSubject, $testObject4, 3);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject3, $testObject2, $testObject4, $testObject1), $objects);

        $graph->add_resource_to_sequence_in_position($testSubject, $testObject5, count($objects)+1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject3, $testObject2, $testObject4, $testObject1, $testObject5), $objects);
    }

    public function testAddToSequenceInPositionAgain()
    {
        $graph = new ExtendedGraph();
        $graph->add_resource_to_sequence('http://seq', 'http://item/1');
        $graph->add_resource_to_sequence('http://seq', 'http://item/2');
        $graph->add_resource_to_sequence('http://seq', 'http://item/3');
        $graph->add_resource_to_sequence('http://seq', 'http://item/4');

        $graph->add_resource_to_sequence_in_position('http://seq', 'http://item/new', 2);

        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf.'_1', 'http://item/1'));
        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf.'_2', 'http://item/new'));
        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf.'_3', 'http://item/2'));
        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf.'_4', 'http://item/3'));
        $this->assertTrue($graph->has_resource_triple('http://seq', ExtendedGraph::rdf.'_5', 'http://item/4'));
    }

    public function testAddLiteralToSequence()
    {
        $testSubject = 'http://some/subject/s1';
        $testObject1 = 'foo1';
        $testObject2 = 'foo2';

        $graph = new ExtendedGraph();
        $graph->add_literal_to_sequence($testSubject, $testObject1);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject1), $objects);

        $graph->add_literal_to_sequence($testSubject, $testObject2);

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject1, $testObject2), $objects);

        $graph->add_literal_to_sequence('http://some/other/subject', 'bar');

        $objects = $graph->get_sequence_values('http://some/subject/s1');
        $this->assertEquals(array($testObject1, $testObject2), $objects);

        $objects = $graph->get_sequence_values('http://some/other/subject');
        $this->assertEquals(array('bar'), $objects);
    }

    public function testGetTripleCountWithNoParams(){
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');
        $expected = 3;
        $actual = $graph->get_triple_count();
        $this->assertEquals($expected, $actual);
    }
    public function testGetTripleCountWithSubject(){
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');
        $expected = 1;
        $actual = $graph->get_triple_count('http://some/subject/1');
        $this->assertEquals($expected, $actual);
    }
    public function testGetTripleCountWithPredicate(){
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');
        $expected = 3;
        $actual = $graph->get_triple_count(false, 'http://some/predicate');
        $this->assertEquals($expected, $actual);
    }

    public function testGetTripleCountWithObject(){
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');
        $expected = 3;
        $actual = $graph->get_triple_count(false, false, 'some object');
        $this->assertEquals($expected, $actual);
    }

    public function testGetTripleCountWithSubjectandPredicate(){
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'another object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');
        $expected = 2;
        $actual = $graph->get_triple_count('http://some/subject/1', 'http://some/predicate');
        $this->assertEquals($expected, $actual);
    }

    public function testGetTripleCountWithSubjectPredicateAndObject(){
        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'another object');
        $graph->add_literal_triple('http://some/subject/2', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/3', 'http://some/predicate', 'some object');
        $expected = 1;
        $actual = $graph->get_triple_count('http://some/subject/1', 'http://some/predicate', 'some object');
        $this->assertEquals($expected, $actual);
    }

    public function testGetTripleCountWithEmptyGraph(){
        $graph = new ExtendedGraph();
        $expected = 0;
        $actual = $graph->get_triple_count();
        $this->assertEquals($expected, $actual);
    }

    public function testGetTripleCountWithNonExistentSubject(){
        $graph = new ExtendedGraph();
        $expected = 0;
        $actual = $graph->get_triple_count('http://example.com/subject');
        $this->assertEquals($expected, $actual);
    }

    public function testReplaceUris(){
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_literal_triple('http://some/subject/4', 'http://some/predicate', 'http://some/subject/1');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://some/subject/1');
        $graph->replace_uris('http://some/subject/1', 'http://some/subject/3');
        $index = $graph->get_index();
        $this->assertFalse($graph->has_triples_about('http://some/subject/1'), "resource with old uri still exists");
        $this->assertTrue($graph->has_triples_about('http://some/subject/3'), "resource with new uri doesn't exists");
        $this->assertEquals('http://some/subject/3', $index['http://some/subject/2']['http://some/predicate'][0]['value'], "http://some/subject/3");
        $this->assertTrue( $graph->has_literal_triple('http://some/subject/4', 'http://some/predicate', 'http://some/subject/1') , "literal values should not be replaced");

        $graph->replace_uris('http://some/predicate', 'http://some/predicate2');
        $index = $graph->get_index();
        $this->assertTrue(isset($index['http://some/subject/2']['http://some/predicate2']), "predicate should be replaced");
        $this->assertFalse(isset($index['http://some/subject/2']['http://some/predicate']), "predicate should be replaced and old one not be in graph");
    }

    public function testReplaceResourceTriples(){
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate', 'some object');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate', 'http://some/subject/1');
        $graph->replace_resource_triples('http://some/subject/2', 'http://some/predicate', 'http://some/subject/3');
        $index = $graph->get_index();
        $this->assertEquals('http://some/subject/3', $index['http://some/subject/2']['http://some/predicate'][0]['value'], "http://some/subject/3");
    }

    public function testReplaceLiteralTriple(){
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://some/subject/s1', 'http://some/predicate', 'some object');
        $this->assertTrue($graph->replace_literal_triple('http://some/subject/s1', 'http://some/predicate', 'some object', 'replacement object'), 'Should return true on replacement');
        $index = $graph->get_index();
        $this->assertEquals('replacement object', $index['http://some/subject/s1']['http://some/predicate'][0]['value'], "should be 'replacement object'");
    }

    public function testReplaceLiteralTripleReturnsFalseIfNoReplacementMade(){
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://some/subject/s1', 'http://some/predicate', 'some object');
        $this->assertFalse($graph->replace_literal_triple('http://some/othersubject/s1', 'http://some/predicate', 'some object', 'replacement object'), 'Should return FALSE');
    }

    public function testGetResources(){

        $graph = new ExtendedGraph();

        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate/p1', 'some object');
        $graph->add_resource_triple('http://some/subject/2', 'http://some/predicate/p2', 'http://some/object/1');
        $graph->add_resource_triple('http://some/subject/3', 'http://some/predicate/p3', 'http://some/object/2');
        $graph->add_literal_triple('http://some/subject/1', 'http://some/predicate/p3', 'http://this/is/not/a/rsource');

        $expected = array('http://some/subject/1', 'http://some/subject/2', 'http://some/subject/3', 'http://some/object/1', 'http://some/object/2');

        $actual = $graph->get_resources();

        sort($expected);
        sort($actual);

        $this->assertEquals(count($expected), count($actual), 'should get same number of resource uris');
        $this->assertEquals($expected, $actual, 'should get expected array containing all the resource uris');
    }

    public function testGetLabelForUri() {

        $graph = new ExtendedGraph();

        $label = "Wuthering Heights";
        $s1 = 'http://example.com/resource';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph->add_resource_triple($s1, $rdfType, 'http://xmlns.com/foaf/0.1/Person');
        $graph->add_literal_triple($s1, 'http://www.w3.org/2000/01/rdf-schema#label', $label);

        ExtendedGraph::initProperties(array('labelProperties' => array('http://www.w3.org/2000/01/rdf-schema#label')));

        $this->assertEquals($graph->get_label_for_uri($s1), $label, "get_label_for_uri(uri) should return the value of a label property");
    }

    public function testGetLabelForUriLabelPropsNotInitialised(){
    	ExtendedGraph::initProperties(array('labelProperties' => null));
        $this->setExpectedException('Exception','Please initialise ExtendedGraph::$labelProperties');
        $graph = new ExtendedGraph();

        $label = "Wuthering Heights";
        $s1 = 'http://example.com/resource';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph->add_resource_triple($s1, $rdfType, 'http://xmlns.com/foaf/0.1/Person');
        $graph->add_literal_triple($s1, 'http://www.w3.org/2000/01/rdf-schema#label', $label);

        $graph->get_label_for_uri($s1);
    }

    public function testGetLabelForUriReturnsEmptyStringIfSubjectNotFound(){
        $graph = new ExtendedGraph();

        $label = "Wuthering Heights";
        $s1 = 'http://example.com/resource';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph->add_resource_triple($s1, $rdfType, 'http://xmlns.com/foaf/0.1/Person');
        $graph->add_literal_triple($s1, 'http://www.w3.org/2000/01/rdf-schema#label', $label);

        ExtendedGraph::initProperties(array('labelProperties' => array('http://www.w3.org/2000/01/rdf-schema#label')));

        $this->assertEquals($graph->get_label_for_uri("http://example.com/1"), '');

    }

    public function testGetLabelForUriReturnsEmptyStringLabelNotFound(){
        $graph = new ExtendedGraph();

        $label = "Wuthering Heights";
        $s1 = 'http://example.com/resource';
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph->add_resource_triple($s1, $rdfType, 'http://xmlns.com/foaf/0.1/Person');
        $graph->add_literal_triple($s1, 'http://www.w3.org/2000/01/rdf-schema#label', $label);

        ExtendedGraph::initProperties(array('labelProperties' => array('http://www.w3.org/2000/01/rdf-schema#label2')));

        $this->assertEquals($graph->get_label_for_uri($s1), '');

    }

    function testIsEqualToReturnsTrueForIdenticalGraphs()
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

    function testIsEqualToReturnsFalseForDifferingGraphs()
    {
        $rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

        $graph1 = new ExtendedGraph();
        $graph1->add_resource_triple('http://example.com/people/bloggs-joe', $rdfType, 'http://xmlns.com/foaf/0.1/Person', 'en');

        $graph2 = new ExtendedGraph();
        $graph2->add_resource_triple('http://example.com/people/mouse-mickey', $rdfType, 'http://xmlns.com/foaf/0.1/Person', 'en');

        $graph3 = new ExtendedGraph();
        $graph3->add_literal_triple('http://example.com/people/mouse-mickey', $rdfType, 'http://xmlns.com/foaf/0.1/Person', 'en');

        $graph4 = new ExtendedGraph();
        $graph4->add_resource_triple('http://example.com/people/mouse-mickey', 'http://some/predicate/p1', 'http://xmlns.com/foaf/0.1/Person', 'en');

        $graph5 = new ExtendedGraph();
        $graph5->add_resource_triple('http://example.com/people/mouse-mickey', $rdfType, 'http://xmlns.com/foaf/0.1/Mouse', 'en');

        $graph6 = new ExtendedGraph();
        $graph6->add_resource_triple('http://example.com/people/mouse-mickey', $rdfType, 'http://xmlns.com/foaf/0.1/Person', 'de');

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

    function testIsEqualToIgnoresNamespaceDifferences()
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

    public function testRemoveResourceFromSequence()
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
        $this->assertEquals(array($sub1, $sub3), $sequenceValues, 'There should be two sequence values, in the correct order');
        $this->assertTrue($graph->has_resource_triple($s, ExtendedGraph::rdf."_1", $sub1));
        $this->assertTrue($graph->has_resource_triple($s, ExtendedGraph::rdf."_2", $sub3));
    }

    public function testFromGraph()
    {
        $itemUri = 'http://foo/item';
        $mainResourceUri = 'http://foo/mainResource';
        $partOfResourceUri = 'http://foo/partOfResource';

        $itemGraph = new ExtendedGraph();

        $itemGraph->add_resource_triple($itemUri, ExtendedGraph::rdf.'type', self::ONT_resource.'Item');
        $itemGraph->add_resource_triple($itemUri, self::ONT_resource.'resource', $mainResourceUri);

        $graph = new ExtendedGraph();
        $graph->from_graph($itemGraph);

        $this->assertTrue($graph->is_equal_to($itemGraph));
    }

    public function testRemoveSubjectsOfType()
    {
        $graph = new ExtendedGraph();
        $graph->add_resource_triple("http://test/1", ExtendedGraph::rdf.'type', self::ONT_resource.'Item');
        $graph->add_resource_triple("http://test/2", ExtendedGraph::rdf.'type', self::ONT_resource.'Item');
        $graph->add_resource_triple("http://test/3", ExtendedGraph::rdf.'type', self::ONT_resource.'List');

        $graph->remove_subjects_of_type(self::ONT_resource.'Item');

        $subjects = $graph->get_subjects();
        $this->assertEquals(1, count($subjects));
        $this->assertEquals("http://test/3", $subjects[0]);
    }

    public function testReplaceLiteralTriples()
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple("http://test/1", 'http://www.w3.org/2000/01/rdf-schema#label', "value1");
        $graph->add_literal_triple("http://test/1", 'http://www.w3.org/2000/01/rdf-schema#label', "value2");

        $graph->replace_literal_triples("http://test/1", 'http://www.w3.org/2000/01/rdf-schema#label', "value3");
        $this->assertTrue($graph->has_literal_triple( "http://test/1", 'http://www.w3.org/2000/01/rdf-schema#label', "value3"));
        $this->assertFalse($graph->has_literal_triple("http://test/1", 'http://www.w3.org/2000/01/rdf-schema#label', "value1"));
        $this->assertFalse($graph->has_literal_triple("http://test/1", 'http://www.w3.org/2000/01/rdf-schema#label', "value2"));
    }
}
?>