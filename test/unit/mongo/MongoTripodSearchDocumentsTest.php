<?php
class MongoTripodSearchDocumentsTest extends MongoTripodTestBase
{
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

}