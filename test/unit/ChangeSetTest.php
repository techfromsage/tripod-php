<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Tripod\ChangeSet;

class ChangeSetTest extends TestCase
{
    private const CS = 'http://purl.org/vocab/changeset/schema#';

    private const SUBJECT = 'http://example.com/resources/1';

    public function testEmptyChangeSetHasNoChanges(): void
    {
        $cs = new ChangeSet([]);
        $this->assertFalse($cs->has_changes());
    }

    public function testChangeSetFromBeforeAndAfterTriplesArrays(): void
    {
        $cs = new ChangeSet([
            'before' => [$this->triple(self::SUBJECT, 'http://purl.org/dc/terms/title', 'Old title')],
            'after' => [$this->triple(self::SUBJECT, 'http://purl.org/dc/terms/title', 'New title')],
            'changeReason' => 'testing',
            'creatorName' => 'unit test',
            'createdDate' => '2020-01-01T00:00:00Z',
        ]);

        $this->assertTrue($cs->has_changes());
        $this->assertSame([self::SUBJECT], $cs->get_subjects_of_change());

        // one changeset resource with an addition and a removal
        $changeSets = $cs->get_subjects_of_type(self::CS . 'ChangeSet');
        $this->assertCount(1, $changeSets);
        $csId = $changeSets[0];
        $this->assertNotEmpty($cs->get_first_resource($csId, self::CS . 'addition'));
        $this->assertNotEmpty($cs->get_first_resource($csId, self::CS . 'removal'));
        $this->assertSame('testing', $cs->get_first_literal($csId, self::CS . 'changeReason'));
        $this->assertSame('unit test', $cs->get_first_literal($csId, self::CS . 'creatorName'));
        $this->assertSame('2020-01-01T00:00:00Z', $cs->get_first_literal($csId, self::CS . 'createdDate'));
    }

    public function testChangeSetWithOnlyAdditions(): void
    {
        $cs = new ChangeSet([
            'after' => [$this->triple(self::SUBJECT, 'http://purl.org/dc/terms/title', 'New title')],
        ]);

        $this->assertTrue($cs->has_changes());
        $changeSets = $cs->get_subjects_of_type(self::CS . 'ChangeSet');
        $this->assertCount(1, $changeSets);
        $this->assertNotEmpty($cs->get_first_resource($changeSets[0], self::CS . 'addition'));
        $this->assertNull($cs->get_first_resource($changeSets[0], self::CS . 'removal'));
    }

    public function testChangeSetWithOnlyRemovals(): void
    {
        $cs = new ChangeSet([
            'before' => [$this->triple(self::SUBJECT, 'http://purl.org/dc/terms/title', 'Old title')],
        ]);

        $this->assertTrue($cs->has_changes());
        $changeSets = $cs->get_subjects_of_type(self::CS . 'ChangeSet');
        $this->assertCount(1, $changeSets);
        $this->assertNull($cs->get_first_resource($changeSets[0], self::CS . 'addition'));
        $this->assertNotEmpty($cs->get_first_resource($changeSets[0], self::CS . 'removal'));
    }

    public function testIdenticalBeforeAndAfterProducesNoChanges(): void
    {
        $triples = [$this->triple(self::SUBJECT, 'http://purl.org/dc/terms/title', 'Same title')];
        $cs = new ChangeSet(['before' => $triples, 'after' => $triples]);
        $this->assertFalse($cs->has_changes());
    }

    public function testEtagTriplesAreIgnored(): void
    {
        $cs = new ChangeSet([
            'before' => [$this->triple(self::SUBJECT, 'http://schemas.talis.com/2005/dir/schema#etag', 'abc')],
            'after' => [$this->triple(self::SUBJECT, 'http://schemas.talis.com/2005/dir/schema#etag', 'def')],
        ]);

        $this->assertFalse($cs->has_changes());
    }

    public function testChangeSetFromRdfXml(): void
    {
        $beforeRdfXml = '<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:dct="http://purl.org/dc/terms/">
  <rdf:Description rdf:about="' . self::SUBJECT . '">
    <dct:title>Old title</dct:title>
  </rdf:Description>
</rdf:RDF>';
        $afterRdfXml = str_replace('Old title', 'New title', $beforeRdfXml);

        $cs = new ChangeSet([
            'before_rdfxml' => $beforeRdfXml,
            'after_rdfxml' => $afterRdfXml,
        ]);

        $this->assertTrue($cs->has_changes());
        $this->assertSame([self::SUBJECT], $cs->get_subjects_of_change());
    }

    public function testMultipleSubjectsOfChange(): void
    {
        $cs = new ChangeSet([
            'before' => [
                $this->triple('http://example.com/resources/1', 'http://purl.org/dc/terms/title', 'One'),
                $this->triple('http://example.com/resources/2', 'http://purl.org/dc/terms/title', 'Two'),
            ],
            'after' => [
                $this->triple('http://example.com/resources/1', 'http://purl.org/dc/terms/title', 'One updated'),
                $this->triple('http://example.com/resources/2', 'http://purl.org/dc/terms/title', 'Two updated'),
            ],
        ]);

        $subjects = $cs->get_subjects_of_change();
        sort($subjects);
        $this->assertSame(['http://example.com/resources/1', 'http://example.com/resources/2'], $subjects);
        $this->assertCount(2, $cs->get_subjects_of_type(self::CS . 'ChangeSet'));
    }

    public function testSubjectOfChangeRestrictsChangeSets(): void
    {
        $cs = new ChangeSet([
            'subjectOfChange' => 'http://example.com/resources/1',
            'before' => [
                $this->triple('http://example.com/resources/1', 'http://purl.org/dc/terms/title', 'One'),
            ],
            'after' => [
                $this->triple('http://example.com/resources/1', 'http://purl.org/dc/terms/title', 'One updated'),
            ],
        ]);

        $this->assertSame(['http://example.com/resources/1'], $cs->get_subjects_of_change());
    }

    public function testDcSourceIsAddedToStatements(): void
    {
        $cs = new ChangeSet([
            'after' => [$this->triple(self::SUBJECT, 'http://purl.org/dc/terms/title', 'New title')],
            'http://purl.org/dc/terms/source' => 'http://example.com/provenance',
        ]);

        $changeSets = $cs->get_subjects_of_type(self::CS . 'ChangeSet');
        $addition = $cs->get_first_resource($changeSets[0], self::CS . 'addition');
        $this->assertSame(
            'http://example.com/provenance',
            $cs->get_first_resource($addition, 'http://purl.org/dc/terms/source')
        );
    }

    public function testExtraPropertiesAreAddedToChangeSets(): void
    {
        $cs = new ChangeSet([
            'after' => [$this->triple(self::SUBJECT, 'http://purl.org/dc/terms/title', 'New title')],
            'properties' => [
                'http://example.com/vocab#batch' => [['value' => 'batch-1', 'type' => 'literal']],
            ],
        ]);

        $changeSets = $cs->get_subjects_of_type(self::CS . 'ChangeSet');
        $this->assertSame('batch-1', $cs->get_first_literal($changeSets[0], 'http://example.com/vocab#batch'));
    }

    public function testToRdfXml(): void
    {
        $cs = new ChangeSet([
            'before' => [$this->triple(self::SUBJECT, 'http://purl.org/dc/terms/title', 'Old title')],
            'after' => [$this->triple(self::SUBJECT, 'http://purl.org/dc/terms/title', 'New title')],
        ]);

        $rdfxml = $cs->toRDFXML();
        $this->assertStringContainsString('ChangeSet', $rdfxml);
        $this->assertStringContainsString(self::SUBJECT, $rdfxml);
        $this->assertStringContainsString('New title', $rdfxml);
        $this->assertStringContainsString('Old title', $rdfxml);
        // snake_case alias returns the same serialisation
        $this->assertSame($rdfxml, $cs->to_rdfxml());
    }

    /**
     * @return array triples array in the format the ChangeSet constructor accepts
     */
    private function triple(string $s, string $p, string $o, string $type = 'literal'): array
    {
        return ['s' => $s, 'p' => $p, 'o' => $o, 'o_type' => $type];
    }
}
