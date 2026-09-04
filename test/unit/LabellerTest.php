<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Tripod\ExtendedGraph;
use Tripod\Labeller;

class LabellerTest extends TestCase
{
    private Labeller $labeller;

    protected function setUp(): void
    {
        $this->labeller = new Labeller();
    }

    public function testQnameToUri(): void
    {
        $this->assertSame('http://purl.org/dc/terms/title', $this->labeller->qname_to_uri('dct:title'));
    }

    public function testQnameToUriReturnsNullForUnknownPrefix(): void
    {
        $this->assertNull($this->labeller->qname_to_uri('unknown:foo'));
    }

    public function testQnameToUriReturnsNullForNonQname(): void
    {
        $this->assertNull($this->labeller->qname_to_uri('notaqname'));
        $this->assertNull($this->labeller->qname_to_uri(null));
    }

    public function testUriToQnameWithRegisteredNamespace(): void
    {
        $this->assertSame('dct:title', $this->labeller->uri_to_qname('http://purl.org/dc/terms/title'));
    }

    public function testUriToQnameGeneratesPrefixFromUriPart(): void
    {
        $this->assertSame('myvocab:thing', $this->labeller->uri_to_qname('http://example.com/myvocab/thing'));
        // the generated prefix is registered for subsequent conversions
        $this->assertSame('http://example.com/myvocab/wibble', $this->labeller->qname_to_uri('myvocab:wibble'));
    }

    public function testUriToQnameReturnsNullForUnparseableUri(): void
    {
        $this->assertNull($this->labeller->uri_to_qname('nonsense'));
        $this->assertNull($this->labeller->uri_to_qname(null));
    }

    public function testSetNamespaceMapping(): void
    {
        $this->labeller->set_namespace_mapping('ex', 'http://example.com/terms#');
        $this->assertSame('http://example.com/terms#foo', $this->labeller->qname_to_uri('ex:foo'));
        $this->assertSame('ex:foo', $this->labeller->uri_to_qname('http://example.com/terms#foo'));
        $this->assertArrayHasKey('ex', $this->labeller->get_ns());
    }

    public function testGetPrefixReturnsExistingPrefix(): void
    {
        $this->assertSame('dct', $this->labeller->get_prefix('http://purl.org/dc/terms/'));
    }

    public function testGetPrefixSkipsReservedUriParts(): void
    {
        // 'schema' is skipped, so the prefix is derived from the next eligible part
        $this->assertSame('alpha', $this->labeller->get_prefix('http://example.org/alpha/schema#'));
    }

    public function testGetPrefixFallsBackToGeneratedPrefix(): void
    {
        // no eligible URI part (hostname contains a dot, 'ns' is reserved)
        $prefix = $this->labeller->get_prefix('http://foo.io/ns#');
        $this->assertSame('msg0', $prefix);
        $ns = $this->labeller->get_ns();
        $this->assertSame('http://foo.io/ns#', $ns[$prefix]);
    }

    public function testGetLabelFromLabelsTable(): void
    {
        $this->assertSame('date created', $this->labeller->get_label('http://purl.org/dc/terms/created'));
        $this->assertSame('Date created', $this->labeller->get_label('http://purl.org/dc/terms/created', null, true));
    }

    public function testGetLabelForRdfContainerMembershipProperty(): void
    {
        // _1 to _20 are in the labels table; higher ordinals hit the pattern match
        $this->assertSame('item 21', $this->labeller->get_label('http://www.w3.org/1999/02/22-rdf-syntax-ns#_21'));
        $this->assertSame('Item 21', $this->labeller->get_label('http://www.w3.org/1999/02/22-rdf-syntax-ns#_21', null, true));
    }

    public function testGetLabelFromGraph(): void
    {
        $uri = 'http://example.com/thing';
        $graph = new ExtendedGraph();
        $graph->add_literal_triple($uri, 'http://www.w3.org/2004/02/skos/core#prefLabel', 'My Thing', 'en');
        $this->assertSame('My Thing', $this->labeller->get_label($uri, $graph));
    }

    public function testGetLabelFromGraphFallsBackThroughLabelProperties(): void
    {
        $uri = 'http://example.com/thing';
        $graph = new ExtendedGraph();
        $graph->add_literal_triple($uri, FOAF_NAME, 'A Name', 'en');
        $this->assertSame('A Name', $this->labeller->get_label($uri, $graph));
    }

    public function testGetLabelUsesCustomLabellingProperty(): void
    {
        $uri = 'http://example.com/thing';
        $this->labeller->add_labelling_property('http://example.com/vocab#customLabel');
        $graph = new ExtendedGraph();
        $graph->add_literal_triple($uri, 'http://example.com/vocab#customLabel', 'Custom Label', 'en');
        $this->assertSame('Custom Label', $this->labeller->get_label($uri, $graph));
    }

    public function testGetLabelSplitsCamelCaseLocalname(): void
    {
        $this->assertSame('first name', $this->labeller->get_label('http://example.com/vocab#hasFirstName'));
        $this->assertSame('First name', $this->labeller->get_label('http://example.com/vocab#hasFirstName', null, true));
    }

    public function testGetLabelFallsBackToLocalname(): void
    {
        $this->assertSame('thing', $this->labeller->get_label('http://example.com/vocab#thing'));
        $this->assertSame('Thing', $this->labeller->get_label('http://example.com/vocab#thing', null, true));
    }

    public function testGetLabelWithQnames(): void
    {
        $this->assertSame('dct:extent', $this->labeller->get_label('http://purl.org/dc/terms/extent', null, false, true));
    }

    public function testGetLabelReturnsUriWhenNothingMatches(): void
    {
        $this->assertSame('urn:isbn', $this->labeller->get_label('urn:isbn'));
    }

    public function testGetPluralLabelFromLabelsTable(): void
    {
        $this->assertSame('dates created', $this->labeller->get_plural_label('http://purl.org/dc/terms/created'));
        $this->assertSame('Dates created', $this->labeller->get_plural_label('http://purl.org/dc/terms/created', null, true));
    }

    public function testGetPluralLabelPluralisesSingleEntryLabel(): void
    {
        $this->assertSame('phone numbers', $this->labeller->get_plural_label('http://xmlns.com/foaf/0.1/phone'));
    }

    public function testGetPluralLabelFromGraph(): void
    {
        $uri = 'http://example.com/thing';
        $graph = new ExtendedGraph();
        $graph->add_literal_triple($uri, 'http://purl.org/net/vocab/2004/03/label#plural', 'thingies', 'en');
        $this->assertSame('thingies', $this->labeller->get_plural_label($uri, $graph));
    }

    public function testGetPluralLabelPluralisesLowercaseLocalname(): void
    {
        $this->assertSame('cats', $this->labeller->get_plural_label('http://example.com/vocab#cat'));
    }

    public function testGetPluralLabelWithQnames(): void
    {
        $this->assertSame('dct:extents', $this->labeller->get_plural_label('http://purl.org/dc/terms/extent', null, false, true));
    }

    public function testGetPluralLabelLeavesMultiWordLabelAlone(): void
    {
        $this->assertSame('first name', $this->labeller->get_plural_label('http://example.com/vocab#hasFirstName'));
    }

    public function testGetInverseLabelFromLabelsTable(): void
    {
        $this->assertSame('is date created of', $this->labeller->get_inverse_label('http://purl.org/dc/terms/created'));
        $this->assertSame('Is date created of', $this->labeller->get_inverse_label('http://purl.org/dc/terms/created', null, true));
    }

    public function testGetInverseLabelDerivedFromSingleEntryLabel(): void
    {
        $this->assertSame('is phone number of', $this->labeller->get_inverse_label('http://xmlns.com/foaf/0.1/phone'));
    }

    public function testGetInverseLabelFromGraph(): void
    {
        $uri = 'http://example.com/thing';
        $graph = new ExtendedGraph();
        $graph->add_literal_triple($uri, 'http://purl.org/net/vocab/2004/03/label#inverseSingular', 'is thing of', 'en');
        $this->assertSame('is thing of', $this->labeller->get_inverse_label($uri, $graph));
    }

    public function testGetInverseLabelFallsBackToGeneratedLabel(): void
    {
        $this->assertSame('is cat of', $this->labeller->get_inverse_label('http://example.com/vocab#cat'));
        $this->assertSame('Is cat of', $this->labeller->get_inverse_label('http://example.com/vocab#cat', null, true));
    }

    public function testLabelGraphAddsLabelsForKnownProperties(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/thing', 'http://purl.org/dc/terms/created', '2020-01-01');

        $this->labeller->label_graph($graph);

        $predicate = 'http://purl.org/dc/terms/created';
        $this->assertSame('date created', $graph->get_first_literal($predicate, RDFS_LABEL));
        $this->assertSame('dates created', $graph->get_first_literal($predicate, 'http://purl.org/net/vocab/2004/03/label#plural'));
        $this->assertSame('is date created of', $graph->get_first_literal($predicate, 'http://purl.org/net/vocab/2004/03/label#inverseSingular'));
    }

    public function testLabelGraphDerivesPluralAndInverseForSingleEntryLabels(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/thing', 'http://xmlns.com/foaf/0.1/phone', '0123456789');

        $this->labeller->label_graph($graph);

        $predicate = 'http://xmlns.com/foaf/0.1/phone';
        $this->assertSame('phone number', $graph->get_first_literal($predicate, RDFS_LABEL));
        $this->assertSame('phone numbers', $graph->get_first_literal($predicate, 'http://purl.org/net/vocab/2004/03/label#plural'));
        $this->assertSame('is phone number of', $graph->get_first_literal($predicate, 'http://purl.org/net/vocab/2004/03/label#inverseSingular'));
    }

    public function testLabelGraphLabelsContainerMembershipProperties(): void
    {
        $graph = new ExtendedGraph();
        $graph->add_resource_triple('http://example.com/list', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#_21', 'http://example.com/item');

        $this->labeller->label_graph($graph);

        $this->assertSame(
            'Item 21',
            $graph->get_first_literal('http://www.w3.org/1999/02/22-rdf-syntax-ns#_21', RDFS_LABEL)
        );
    }

    public function testLabelGraphDoesNotOverwriteExistingLabels(): void
    {
        $predicate = 'http://purl.org/dc/terms/created';
        $graph = new ExtendedGraph();
        $graph->add_literal_triple('http://example.com/thing', $predicate, '2020-01-01');
        $graph->add_literal_triple($predicate, RDFS_LABEL, 'my own label');

        $this->labeller->label_graph($graph);

        $this->assertSame(['my own label'], $graph->get_literal_triple_values($predicate, RDFS_LABEL));
    }
}
