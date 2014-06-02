<?php
require_once dirname(__FILE__).'/../TripodTestBase.php';

class MongoTripodSearchDocumentsTest extends TripodTestBase
{
	protected function setUp()
	{
		parent::setUp();
	
		$this->tripod = new MongoTripod('CBD_testing', 'testing');
		$this->tripod->collection->drop();
        $this->loadBaseSearchDataViaTripod();
        $this->tripod->db->selectCollection(SEARCH_INDEX_COLLECTION)->drop();
        $this->tripod->db->selectCollection("CBD_testing");
	}
	
	public function testGenerateSearchDocumentBasedOnSpecIdThrowsExceptionWithEmptyResource()
	{
		$this->setExpectedException("Exception","Resource must be specified");		
		$searchDocuments = new MongoTripodSearchDocuments($this->tripod->db, $this->tripod->collection, 'http://talisaspire.com/');
		$searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', null, 'http://talisaspire.com/');
	}
	
	public function testGenerateSearchDocumentBasedOnSpecIdThrowsExceptionWithEmptyContext()
	{
		$this->setExpectedException("Exception","Context must be specified");
		$searchDocuments = new MongoTripodSearchDocuments($this->tripod->db, $this->tripod->collection, 'http://talisaspire.com/');
		$searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resource/1', null);
	}
	
	public function testGenerateSearchDocumentBasedOnSpecIdReturnNullForInvalidSearchSpecId()
	{
		$mockSearchDocuments = $this->getMock('MongoTripodSearchDocuments', 
											array('getSearchDocumentSpecification'), 
											array($this->tripod->db, $this->tripod->collection, 'http://talisaspire.com/'));
		
		$mockSearchDocuments->expects($this->once())
							->method('getSearchDocumentSpecification')
							->will($this->returnValue(null));
		$generatedDocuments = $mockSearchDocuments->generateSearchDocumentBasedOnSpecId('i_search_something', 'http://talisaspire.com/resource/1', 'http://talisaspire.com/');
		$this->assertNull($generatedDocuments);
	}
	
	public function testGenerateSearchDocumentBasedOnSpecIdReturnNullIfNoMatchForResourceFound()
	{
		$searchDocuments = new MongoTripodSearchDocuments($this->tripod->db, $this->tripod->collection, 'http://talisaspire.com/');
		$generatedDocuments = $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resource/1', 'http://talisaspire.com/');
		$this->assertNull($generatedDocuments);
	}
	
	public function testGenerateSearchDocumentBasedOnSpecId()
	{	
		$searchDocuments = new MongoTripodSearchDocuments($this->tripod->db, $this->tripod->collection, 'http://talisaspire.com/');		
		$generatedDocuments = $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resources/doc1', 'http://talisaspire.com/');	
		$this->assertEquals('http://talisaspire.com/resources/doc1' , $generatedDocuments['_id']['r']);
	}
	
	
	
	public function testGenerateSearchDocumentBasedOnSpecIdWithFieldNamePredicatesHavingNoValueInCollection()
	{
		$searchSpecs = json_decode('{"_id":"i_search_resource","type":["bibo:Book"],"from":"CBD_testing","filter":[{"condition":{"dct:title.l":{"$exists":true}}}],"indices":[{"fieldName":"search_terms","predicates":["dct:title","dct:subject"]},{"fieldName":"other_terms","predicates":["rdf:type"]}],"fields":[{"fieldName":"result.title","predicates":["dct:title"],"limit":1},{"fieldName":"result.link","value":"_link_"},{"fieldName":"rdftype","predicates":["rdf:type"],"limit":1}],"joins":{"dct:creator":{"indices":[{"fieldName":"search_terms","predicates":["foaf:name"]}],"fields":[{"fieldName":"result.author","predicates":["foaf:name"],"limit":1}, {"fieldName":"result.role","predicates":["siocAccess:Role"], "limit":1}] } }}', true);
		$mockSearchDocuments = $this->getMock('MongoTripodSearchDocuments',
				array('getSearchDocumentSpecification'),
				array($this->tripod->db, $this->tripod->collection, 'http://talisaspire.com/'));
		$mockSearchDocuments->expects($this->once())
							->method('getSearchDocumentSpecification')
							->will($this->returnValue($searchSpecs));
		
		$generatedDocuments = $mockSearchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resources/doc1', 'http://talisaspire.com/');
		$this->assertNotNull($generatedDocuments);
		$this->assertEquals('http://talisaspire.com/resources/doc1' , $generatedDocuments['_id']['r']);
	}
}