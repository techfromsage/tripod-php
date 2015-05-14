<?php
require_once 'MongoTripodTestBase.php';

class MongoTripodSearchDocumentsTest extends MongoTripodTestBase
{
    protected $defaultContext = 'http://talisaspire.com/';

    protected $defaultStoreName = 'tripod_php_testing';

    protected $defaultPodName = 'CBD_testing';

	protected function setUp()
	{
		parent::setUp();
	
		$this->tripod = new MongoTripod('CBD_testing', 'tripod_php_testing');
		$this->getTripodCollection($this->tripod)->drop();
        $this->loadBaseSearchDataViaTripod();
        foreach(MongoTripodConfig::getInstance()->getCollectionsForSearch($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop();
        }
	}

    protected function getMongoTripodSearchDocuments(MongoTripod $tripod)
    {
        return new MongoTripodSearchDocuments(
            $tripod->getStoreName(),
            $this->getTripodCollection($tripod),
            'http://talisaspire.com/'
        );
    }
	
	public function testGenerateSearchDocumentBasedOnSpecIdThrowsExceptionWithEmptyResource()
	{
		$this->setExpectedException("Exception","Resource must be specified");		
		$searchDocuments = $this->getMongoTripodSearchDocuments($this->tripod);
		$searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', null, 'http://talisaspire.com/');
	}
	
	public function testGenerateSearchDocumentBasedOnSpecIdThrowsExceptionWithEmptyContext()
	{
		$this->setExpectedException("Exception","Context must be specified");
        $searchDocuments = $this->getMongoTripodSearchDocuments($this->tripod);
		$searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resource/1', null);
	}
	
	public function testGenerateSearchDocumentBasedOnSpecIdReturnNullForInvalidSearchSpecId()
	{
		$mockSearchDocuments = $this->getMock('MongoTripodSearchDocuments',
											array('getSearchDocumentSpecification'), 
											array($this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'));
		
		$mockSearchDocuments->expects($this->once())
							->method('getSearchDocumentSpecification')
							->will($this->returnValue(null));
		$generatedDocuments = $mockSearchDocuments->generateSearchDocumentBasedOnSpecId('i_search_something', 'http://talisaspire.com/resource/1', 'http://talisaspire.com/');
		$this->assertNull($generatedDocuments);
	}
	
	public function testGenerateSearchDocumentBasedOnSpecIdReturnNullIfNoMatchForResourceFound()
	{
        $searchDocuments = $this->getMongoTripodSearchDocuments($this->tripod);
        $generatedDocuments = $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resource/1', 'http://talisaspire.com/');
		$this->assertNull($generatedDocuments);
	}
	
	public function testGenerateSearchDocumentBasedOnSpecId()
	{
        $searchDocuments = $this->getMongoTripodSearchDocuments($this->tripod);
		$generatedDocuments = $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resources/doc1', 'http://talisaspire.com/');	
		$this->assertEquals('http://talisaspire.com/resources/doc1' , $generatedDocuments['_id']['r']);
	}
	
	
	
	public function testGenerateSearchDocumentBasedOnSpecIdWithFieldNamePredicatesHavingNoValueInCollection()
	{
		$searchSpecs = json_decode('{"_id":"i_search_resource","type":["bibo:Book"],"from":"CBD_testing","filter":[{"condition":{"dct:title.l":{"$exists":true}}}],"indices":[{"fieldName":"search_terms","predicates":["dct:title","dct:subject"]},{"fieldName":"other_terms","predicates":["rdf:type"]}],"fields":[{"fieldName":"result.title","predicates":["dct:title"],"limit":1},{"fieldName":"result.link","value":"link"},{"fieldName":"rdftype","predicates":["rdf:type"],"limit":1}],"joins":{"dct:creator":{"indices":[{"fieldName":"search_terms","predicates":["foaf:name"]}],"fields":[{"fieldName":"result.author","predicates":["foaf:name"],"limit":1}, {"fieldName":"result.role","predicates":["siocAccess:Role"], "limit":1}] } }}', true);
		$mockSearchDocuments = $this->getMock('MongoTripodSearchDocuments',
				array('getSearchDocumentSpecification'),
				array($this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'));
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

        $labeller = new MongoTripodLabeller();
        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri)=>array("dct:subject")
        );

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments($uri, $this->defaultContext, $this->defaultPodName);

        /** @var MongoTripodSearchIndexer|PHPUnit_Framework_MockObject_MockObject $searchIndexer */
        $searchIndexer = $this->getMock('MongoTripodSearchIndexer',
            array('getSearchDocumentGenerator'),
            array($this->tripod)
        );

        $searchDocuments = $this->getMockBuilder('MongoTripodSearchDocuments')
            ->setMethods(array('generateSearchDocumentBasedOnSpecId'))
            ->setConstructorArgs(
                array(
                    $this->defaultStoreName,
                    MongoTripodConfig::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
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
            new ImpactedSubject(
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

    public function testSearchDocsNotGeneratedWhenUndefinedPredicateChanges()
    {
        $uri = "http://talisaspire.com/resources/doc1";

        $labeller = new MongoTripodLabeller();
        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri)=>array("dct:description")
        );

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments($uri, $this->defaultContext, $this->defaultPodName);
        $impactedSubjects = $this->tripod->getSearchIndexer()->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $this->assertEmpty($impactedSubjects);
    }

    public function testUpdateOfResourceInImpactIndexTriggersRegenerationOfSearchDocs()
    {
        $uri = "http://talisaspire.com/authors/2";
        $labeller = new MongoTripodLabeller();

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments(
            "http://talisaspire.com/resources/doc4",
            $this->defaultContext,
            $this->defaultPodName
        );

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri)=>array('foaf:name')
        );

        /** @var MongoTripodSearchIndexer|PHPUnit_Framework_MockObject_MockObject $searchIndexer */
        $searchIndexer = $this->getMock('MongoTripodSearchIndexer',
            array('getSearchDocumentGenerator'),
            array($this->tripod)
        );

        $searchDocuments = $this->getMockBuilder('MongoTripodSearchDocuments')
            ->setMethods(array('generateSearchDocumentBasedOnSpecId'))
            ->setConstructorArgs(
                array(
                    $this->defaultStoreName,
                    MongoTripodConfig::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
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
            new ImpactedSubject(
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


        $labeller = new MongoTripodLabeller();
        $graph = new ExtendedGraph();
        // This should trigger a search document regeneration, even though issn isn't in the search doc spec
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('baseData:Wibble'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('bibo:issn'), '1234-5678');

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri) => array(
                'rdf:type','bibo:issn'
            )
        );

        /** @var MongoTripod|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            'MongoTripod',
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

        /** @var MongoTripodUpdates|PHPUnit_Framework_MockObject_MockObject $mockTripodUpdates */
        $mockTripodUpdates = $this->getMock(
            'MongoTripodUpdates',
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

        /** @var MongoTripodSearchIndexer|PHPUnit_Framework_MockObject_MockObject $searchIndexer */
        $searchIndexer = $this->getMock('MongoTripodSearchIndexer',
            array('getSearchDocumentGenerator'),
            array($this->tripod)
        );

        $searchDocuments = $this->getMockBuilder('MongoTripodSearchDocuments')
            ->setMethods(array('generateSearchDocumentBasedOnSpecId'))
            ->setConstructorArgs(
                array(
                    $this->defaultStoreName,
                    MongoTripodConfig::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
                    $this->defaultContext
                )
            )->getMock();

        $searchIndexer->expects($this->once())
            ->method('getSearchDocumentGenerator')
            ->will($this->returnValue($searchDocuments));

        $searchDocuments->expects($this->once())
            ->method('generateSearchDocumentBasedOnSpecId')
            ->with('i_search_filter_parse',$labeller->uri_to_alias($uri), $this->defaultContext);

        $mockTripod->saveChanges(new ExtendedGraph(), $graph);

        $impactedSubjects = $searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $expectedImpactedSubjects = array(
            new ImpactedSubject(
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

    public function testUpdateToResourceWithMatchingRdfTypeShouldOnlyRegenerateIfRdfTypeIsPartOfUpdate()
    {
        $uri = "http://talisaspire.com/resources/doc3";
        $labeller = new MongoTripodLabeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $searchIndexer = new MongoTripodSearchIndexer($this->tripod);

        $subjectsAndPredicatesOfChange = array($uriAlias =>array('dct:subject'));

        $this->assertEmpty($searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext));

        $subjectsAndPredicatesOfChange = array($uriAlias =>array('dct:subject', 'rdf:type'));

        $expectedImpactedSubjects = array(
            new ImpactedSubject(
                array(
                    _ID_RESOURCE=>$uriAlias,
                    _ID_CONTEXT=>$this->defaultContext
                ),
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                array()
            )
        );

        $impactedSubjects = $searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);
        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
    }

    public function testNewResourceThatDoesNotMatchAnythingCreatesNoImpactedSubjects()
    {
        $uri = 'http://example.com/resources/' . uniqid();
        $labeller = new MongoTripodLabeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $graph = new ExtendedGraph();
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('bibo:Proceedings'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('dct:title'), "A title");

        $subjectsAndPredicatesOfChange = array($uriAlias=>array('rdf:type', 'dct:title'));

        /** @var MongoTripod|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            'MongoTripod',
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

        /** @var MongoTripodUpdates|PHPUnit_Framework_MockObject_MockObject $mockTripodUpdates */
        $mockTripodUpdates = $this->getMock(
            'MongoTripodUpdates',
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

        $mockTripod->saveChanges(new ExtendedGraph(), $graph);

        $searchIndexer = $mockTripod->getComposite(OP_SEARCH);

        $this->assertEmpty($searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext));
    }

    public function testDeleteResourceCreatesImpactedSubjects()
    {
        $uri = 'http://talisaspire.com/resources/doc5';
        $labeller = new MongoTripodLabeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments($uriAlias, $this->defaultContext, $this->defaultPodName);

        $subjectsAndPredicatesOfChange = array($uriAlias=>array());

        $searchIndexer = new MongoTripodSearchIndexer($this->tripod);

        $expectedImpactedSubjects = array(
            new ImpactedSubject(
                array(
                    _ID_RESOURCE=>$uriAlias,
                    _ID_CONTEXT=>$this->defaultContext
                ),
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                array('i_search_resource')
            )
        );
        $this->assertEquals($expectedImpactedSubjects, $searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext));
    }

}