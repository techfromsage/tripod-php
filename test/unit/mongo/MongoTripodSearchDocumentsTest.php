<?php
require_once 'MongoTripodTestBase.php';

use \Tripod\Mongo\SearchDocuments;

class MongoTripodSearchDocumentsTest extends MongoTripodTestBase
{
    protected $defaultContext = 'http://talisaspire.com/';

    protected $defaultStoreName = 'tripod_php_testing';

    protected $defaultPodName = 'CBD_testing';

	protected function setUp()
	{
		parent::setUp();

		$this->tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing');
		$this->getTripodCollection($this->tripod)->drop();
        $this->loadBaseSearchDataViaTripod();
        foreach(\Tripod\Mongo\Config::getInstance()->getCollectionsForSearch($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop();
        }
	}

    /**
     * @param \Tripod\Mongo\Driver $tripod
     * @return SearchDocuments
     */
    protected function getSearchDocuments(\Tripod\Mongo\Driver $tripod)
    {
        return new \Tripod\Mongo\SearchDocuments(
            $tripod->getStoreName(),
            $this->getTripodCollection($tripod),
            'http://talisaspire.com/'
        );
    }

	public function testGenerateSearchDocumentBasedOnSpecIdThrowsExceptionWithEmptyResource()
	{
		$this->setExpectedException("Exception","Resource must be specified");
		$searchDocuments = $this->getSearchDocuments($this->tripod);
		$searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', null, 'http://talisaspire.com/');
	}

	public function testGenerateSearchDocumentBasedOnSpecIdThrowsExceptionWithEmptyContext()
	{
		$this->setExpectedException("Exception","Context must be specified");
        $searchDocuments = $this->getSearchDocuments($this->tripod);
		$searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resource/1', null);
	}

	public function testGenerateSearchDocumentBasedOnSpecIdReturnNullForInvalidSearchSpecId()
	{
		$mockSearchDocuments = $this->getMock(
            '\Tripod\Mongo\SearchDocuments',
            array('getSearchDocumentSpecification'),
            array($this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/')
        );

		$mockSearchDocuments->expects($this->once())
							->method('getSearchDocumentSpecification')
							->will($this->returnValue(null));
		$generatedDocuments = $mockSearchDocuments->generateSearchDocumentBasedOnSpecId('i_search_something', 'http://talisaspire.com/resource/1', 'http://talisaspire.com/');
		$this->assertNull($generatedDocuments);
	}

	public function testGenerateSearchDocumentBasedOnSpecIdReturnNullIfNoMatchForResourceFound()
	{
        $searchDocuments = $this->getSearchDocuments($this->tripod);
        $generatedDocuments = $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resource/1', 'http://talisaspire.com/');
		$this->assertNull($generatedDocuments);
	}

	public function testGenerateSearchDocumentBasedOnSpecId()
	{
        $searchDocuments = $this->getSearchDocuments($this->tripod);
		$generatedDocuments = $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resources/doc1', 'http://talisaspire.com/');
		$this->assertEquals('http://talisaspire.com/resources/doc1' , $generatedDocuments['_id']['r']);
	}



	public function testGenerateSearchDocumentBasedOnSpecIdWithFieldNamePredicatesHavingNoValueInCollection()
	{
		$searchSpecs = json_decode('{"_id":"i_search_resource","type":["bibo:Book"],"from":"CBD_testing","filter":[{"condition":{"dct:title.l":{"$exists":true}}}],"indices":[{"fieldName":"search_terms","predicates":["dct:title","dct:subject"]},{"fieldName":"other_terms","predicates":["rdf:type"]}],"fields":[{"fieldName":"result.title","predicates":["dct:title"],"limit":1},{"fieldName":"result.link","value":"link"},{"fieldName":"rdftype","predicates":["rdf:type"],"limit":1}],"joins":{"dct:creator":{"indices":[{"fieldName":"search_terms","predicates":["foaf:name"]}],"fields":[{"fieldName":"result.author","predicates":["foaf:name"],"limit":1}, {"fieldName":"result.role","predicates":["siocAccess:Role"], "limit":1}] } }}', true);

        $mockSearchDocuments = $this->getMock(
            '\Tripod\Mongo\SearchDocuments',
            array('getSearchDocumentSpecification'),
            array($this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/')
        );

		$mockSearchDocuments->expects($this->once())
							->method('getSearchDocumentSpecification')
							->will($this->returnValue($searchSpecs));

		$generatedDocuments = $mockSearchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resources/doc1', 'http://talisaspire.com/');
		$this->assertNotNull($generatedDocuments);
		$this->assertEquals('http://talisaspire.com/resources/doc1' , $generatedDocuments['_id']['r']);
	}

    public function testSearchDocumentsGenerateWhenDefinedPredicateChanges()
    {

        $uri = "http://talisaspire.com/resources/doc1";

        $labeller = new \Tripod\Mongo\Labeller();
        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri)=>array("dct:subject")
        );

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments($uri, $this->defaultContext, $this->defaultPodName);

        /** @var \Tripod\Mongo\Composites\SearchIndexer|PHPUnit_Framework_MockObject_MockObject $searchIndexer */
        $searchIndexer = $this->getMock('\Tripod\Mongo\Composites\SearchIndexer',
            array('getSearchDocumentGenerator'),
            array($this->tripod)
        );

        $searchDocuments = $this->getMockBuilder('\Tripod\Mongo\SearchDocuments')
            ->setMethods(array('generateSearchDocumentBasedOnSpecId'))
            ->setConstructorArgs(
                array(
                    $this->defaultStoreName,
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
                    $this->defaultContext
                )
            )->getMock();

        $searchIndexer->expects($this->once())
            ->method('getSearchDocumentGenerator')
            ->will($this->returnValue($searchDocuments));

        $searchDocuments->expects($this->once())
            ->method('generateSearchDocumentBasedOnSpecId')
            ->with('i_search_resource',$labeller->uri_to_alias($uri), $this->defaultContext);

        $impactedSubjects = $searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>$uri,
                    _ID_CONTEXT=>$this->defaultContext
                ),
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                array('i_search_resource')
            )
        );

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach($impactedSubjects as $subject)
        {
            $searchIndexer->update($subject);
        }
    }

    public function testSearchDocsShouldRegenerateWhenUndefinedPredicateChangesButFilterExistsInSpec()
    {
        $uri = "http://talisaspire.com/resources/doc1";

        $labeller = new Tripod\Mongo\Labeller();
        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri)=>array("dct:description")
        );

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments($uri, $this->defaultContext, $this->defaultPodName);
        $impactedSubjects = $this->tripod->getSearchIndexer()->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $this->assertCount(1, $impactedSubjects);
        $this->assertEquals(
            array(
                _ID_RESOURCE=>$uri,
                _ID_CONTEXT=>"http://talisaspire.com/"
            ),
            $impactedSubjects[0]->getResourceId()
        );

        $this->assertEmpty($impactedSubjects[0]->getSpecTypes());
    }

    public function testUpdateOfResourceInImpactIndexTriggersRegenerationOfSearchDocs()
    {
        $uri = "http://talisaspire.com/authors/2";
        $labeller = new Tripod\Mongo\Labeller();

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments(
            "http://talisaspire.com/resources/doc4",
            $this->defaultContext,
            $this->defaultPodName
        );

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri)=>array('foaf:name')
        );

        /** @var \Tripod\Mongo\Composites\SearchIndexer|PHPUnit_Framework_MockObject_MockObject $searchIndexer */
        $searchIndexer = $this->getMock('\Tripod\Mongo\Composites\SearchIndexer',
            array('getSearchDocumentGenerator'),
            array($this->tripod)
        );

        $searchDocuments = $this->getMockBuilder('\Tripod\Mongo\SearchDocuments')
            ->setMethods(array('generateSearchDocumentBasedOnSpecId'))
            ->setConstructorArgs(
                array(
                    $this->defaultStoreName,
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
                    $this->defaultContext
                )
            )->getMock();

        $searchIndexer->expects($this->once())
            ->method('getSearchDocumentGenerator')
            ->will($this->returnValue($searchDocuments));

        $searchDocuments->expects($this->once())
            ->method('generateSearchDocumentBasedOnSpecId')
            ->with('i_search_resource',$labeller->uri_to_alias("http://talisaspire.com/resources/doc4"), $this->defaultContext);

        $impactedSubjects = $searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>"http://talisaspire.com/resources/doc4",
                    _ID_CONTEXT=>$this->defaultContext
                ),
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                array('i_search_resource')
            )
        );

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach($impactedSubjects as $subject)
        {
            $searchIndexer->update($subject);
        }
    }

    public function testRdfTypeTriggersGenerationOfSearchDocuments()
    {
        $uri = 'http://example.com/resources/' . uniqid();


        $labeller = new Tripod\Mongo\Labeller();
        $graph = new \Tripod\ExtendedGraph();
        // This should trigger a search document regeneration, even though issn isn't in the search doc spec
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('baseData:Wibble'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('bibo:issn'), '1234-5678');

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri) => array(
                'rdf:type','bibo:issn'
            )
        );

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array(
                'getDataUpdater'
            ),
            array(
                $this->defaultPodName,
                $this->defaultStoreName,
                array(
                    'defaultContext'=>$this->defaultContext,
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>true,
                        OP_SEARCH=>false
                    )
                )
            )
        );

        /** @var \Tripod\Mongo\Updates|PHPUnit_Framework_MockObject_MockObject $mockTripodUpdates */
        $mockTripodUpdates = $this->getMock(
            '\Tripod\Mongo\Updates',
            array(
                'processSyncOperations',
                'queueAsyncOperations'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>false,
                        OP_VIEWS=>true,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        /** @var \Tripod\Mongo\Composites\SearchIndexer|PHPUnit_Framework_MockObject_MockObject $searchIndexer */
        $searchIndexer = $this->getMock(
            '\Tripod\Mongo\Composites\SearchIndexer',
            array('getSearchDocumentGenerator'),
            array($this->tripod)
        );

        $searchDocuments = $this->getMockBuilder('\Tripod\Mongo\SearchDocuments')
            ->setMethods(array('generateSearchDocumentBasedOnSpecId'))
            ->setConstructorArgs(
                array(
                    $this->defaultStoreName,
                    \Tripod\Mongo\Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
                    $this->defaultContext
                )
            )->getMock();

        $searchIndexer->expects($this->once())
            ->method('getSearchDocumentGenerator')
            ->will($this->returnValue($searchDocuments));

        $searchDocuments->expects($this->once())
            ->method('generateSearchDocumentBasedOnSpecId')
            ->with('i_search_filter_parse',$labeller->uri_to_alias($uri), $this->defaultContext);

        $mockTripod->saveChanges(new \Tripod\ExtendedGraph(), $graph);

        $impactedSubjects = $searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>$labeller->uri_to_alias($uri),
                    _ID_CONTEXT=>$this->defaultContext
                ),
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                array()
            )
        );

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach($impactedSubjects as $subject)
        {
            $searchIndexer->update($subject);
        }

    }

    public function testNewResourceThatDoesNotMatchAnythingCreatesNoImpactedSubjects()
    {
        $uri = 'http://example.com/resources/' . uniqid();
        $labeller = new Tripod\Mongo\Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $graph = new \Tripod\ExtendedGraph();
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('bibo:Proceedings'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('dct:title'), "A title");

        $subjectsAndPredicatesOfChange = array($uriAlias=>array('rdf:type', 'dct:title'));

        /** @var PHPUnit_Framework_MockObject_MockObject|\Tripod\Mongo\Driver $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
            array(
                'getDataUpdater'
            ),
            array(
                $this->defaultPodName,
                $this->defaultStoreName,
                array(
                    'defaultContext'=>$this->defaultContext,
                    OP_ASYNC=>array(
                        OP_TABLES=>false,
                        OP_VIEWS=>true,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        /** @var \Tripod\Mongo\Updates|PHPUnit_Framework_MockObject_MockObject $mockTripodUpdates */
        $mockTripodUpdates = $this->getMock(
            '\Tripod\Mongo\Updates',
            array(
                'processSyncOperations',
                'queueAsyncOperations'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>true,
                        OP_SEARCH=>false
                    )
                )
            )
        );

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripod->saveChanges(new \Tripod\ExtendedGraph(), $graph);

        $searchIndexer = $mockTripod->getComposite(OP_SEARCH);

        $this->assertEmpty($searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext));
    }

    public function testDeleteResourceCreatesImpactedSubjects()
    {
        $uri = 'http://example.com/resources/' . uniqid();
        $labeller = new Tripod\Mongo\Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $creatorUri = 'http://example.com/identities/oscar-wilde';
        $creatorUriAlias = $labeller->uri_to_alias($creatorUri);

        $graph = new \Tripod\ExtendedGraph();
        $graph->add_resource_triple(
            $uri,
            RDF_TYPE,
            $labeller->qname_to_uri('acorn:Resource')
        );
        $graph->add_resource_triple(
            $uri,
            RDF_TYPE,
            $labeller->qname_to_uri('bibo:Book')
        );
        $graph->add_literal_triple(
            $uri,
            $labeller->qname_to_uri('dct:title'),
            'The Importance of Being Earnest'
        );
        $graph->add_literal_triple(
            $uri,
            $labeller->qname_to_uri('dct:subject'),
            'Plays -- Satire'
        );
        $graph->add_resource_triple(
            $uri,
            $labeller->qname_to_uri('dct:creator'),
            $creatorUri
        );

        $uri2 = 'http://example.com/resources/' . uniqid();
        $uriAlias2 = $labeller->uri_to_alias($uri2);

        $graph2 = new \Tripod\ExtendedGraph();
        $graph2->add_resource_triple(
            $uri2,
            RDF_TYPE,
            $labeller->qname_to_uri('acorn:Resource')
        );
        $graph2->add_resource_triple(
            $uri2,
            RDF_TYPE,
            $labeller->qname_to_uri('bibo:Book')
        );
        $graph2->add_literal_triple(
            $uri2,
            $labeller->qname_to_uri('dct:title'),
            'The Picture of Dorian Gray'
        );
        $graph2->add_literal_triple(
            $uri2,
            $labeller->qname_to_uri('dct:subject'),
            'Portraits -- Fiction'
        );
        $graph2->add_resource_triple(
            $uri2,
            $labeller->qname_to_uri('dct:creator'),
            $creatorUri
        );

        $graph3 = new \Tripod\ExtendedGraph();
        $graph3->add_resource_triple(
            $creatorUri,
            RDF_TYPE,
            $labeller->qname_to_uri('foaf:Person')
        );
        $graph3->add_literal_triple(
            $creatorUri,
            $labeller->qname_to_uri('foaf:name'),
            'Oscar Wilde'
        );

        // Save the graphs and ensure that table rows are generated
        $tripod = new \Tripod\Mongo\Driver(
            $this->defaultPodName,
            $this->defaultStoreName,
            array(
                'defaultContext'=>$this->defaultContext,
                OP_ASYNC=>array(
                    OP_VIEWS=>false,
                    OP_TABLES=>false,
                    OP_SEARCH=>false
                )
            )
        );

        // Save the author graph first so the joins work
        $tripod->saveChanges(new \Tripod\ExtendedGraph(), $graph3);

        $tripod->saveChanges(new \Tripod\ExtendedGraph(), $graph);

        $collection = \Tripod\Mongo\Config::getInstance()->getCollectionForSearchDocument($this->defaultStoreName, 'i_search_resource');

        $query = array(
            _ID_KEY=> array(
                _ID_RESOURCE=>$uriAlias,
                _ID_CONTEXT=>$this->defaultContext,
                _ID_TYPE=>'i_search_resource'
            )
        );
        $this->assertEquals(1, $collection->count($query));

        $tripod->saveChanges(new \Tripod\ExtendedGraph(), $graph2);

        $query[_ID_KEY][_ID_RESOURCE] = $uriAlias2;
        $this->assertEquals(1, $collection->count($query));

        $impactQuery = array(
            _ID_KEY.'.'._ID_TYPE=>'i_search_resource',
            '_impactIndex'=>array(
                _ID_RESOURCE=>$creatorUriAlias,
                _ID_CONTEXT=>$this->defaultContext
            ),
            'result.author'=>'Oscar Wilde'
        );
        $this->assertEquals(2, $collection->count($impactQuery));

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods(array('getDataUpdater'))
            ->setConstructorArgs(
                array(
                    $this->defaultPodName,
                    $this->defaultStoreName,
                    array(
                        'defaultContext'=>$this->defaultContext,
                        OP_ASYNC=>array(
                            OP_VIEWS=>false,
                            OP_TABLES=>false,
                            OP_SEARCH=>false
                        )
                    )
                )
            )->getMock();

        $mockTripodUpdates = $this->getMockBuilder('\Tripod\Mongo\Updates')
            ->setConstructorArgs(
                array(
                    $mockTripod,
                    array(
                        'defaultContext'=>$this->defaultContext,
                        OP_ASYNC=>array(
                            OP_VIEWS=>false,
                            OP_TABLES=>false,
                            OP_SEARCH=>false
                        )
                    )
                )
            )->setMethods(array('processSyncOperations'))
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $expectedSubjectsAndPredicatesOfChange = array(
            $creatorUriAlias=>array('rdf:type','foaf:name')
        );

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $expectedSubjectsAndPredicatesOfChange,
                $this->defaultContext
            );


        // Delete creator resource
        $mockTripod->saveChanges($graph3, new \Tripod\ExtendedGraph());

        $deletedGraph = $mockTripod->describeResource($creatorUri);
        $this->assertTrue($deletedGraph->is_empty());

        // Manually walk through the tables operation
        /** @var \Tripod\Mongo\Composites\SearchIndexer $search */
        $search = $mockTripod->getComposite(OP_SEARCH);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>$uriAlias,
                    _ID_CONTEXT=>$this->defaultContext
                ),
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                array('i_search_resource')
            ),
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>$uriAlias2,
                    _ID_CONTEXT=>$this->defaultContext
                ),
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                array('i_search_resource')
            )
        );

        $this->assertEquals($expectedImpactedSubjects, $search->getImpactedSubjects($expectedSubjectsAndPredicatesOfChange, $this->defaultContext));

        foreach($expectedImpactedSubjects as $subject)
        {
            $search->update($subject);
        }

        $query = array(
            _ID_KEY=> array(
                _ID_RESOURCE=>$uriAlias,
                _ID_CONTEXT=>$this->defaultContext,
                _ID_TYPE=>'i_search_resource'
            )
        );
        $this->assertEquals(1, $collection->count($query));

        $query[_ID_KEY][_ID_RESOURCE] = $uriAlias2;
        $this->assertEquals(1, $collection->count($query));


        // Deleted resource will still be impact indexes because join still exists
        $impactQuery = array(
            _ID_KEY.'.'._ID_TYPE=>'i_search_resource',
            '_impactIndex'=>array(
                _ID_RESOURCE=>$creatorUriAlias,
                _ID_CONTEXT=>$this->defaultContext
            )
        );
        $this->assertEquals(2, $collection->count($impactQuery));

        // But the document should have been regenerated without the value
        $impactQuery['result.author'] = 'Oscar Wilde';
        $this->assertEquals(0, $collection->count($impactQuery));
    }

}