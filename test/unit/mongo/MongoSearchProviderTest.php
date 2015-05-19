<?php
    require_once 'MongoTripodTestBase.php';
    require_once 'src/mongo/Tripod.class.php';
    require_once 'src/mongo/delegates/SearchIndexer.class.php';
    require_once 'src/mongo/providers/MongoSearchProvider.class.php';

use \Tripod\Mongo\Tripod;
use \Tripod\Mongo\SearchIndexer;
use \Tripod\Mongo\MongoSearchProvider;

class MongoSearchProviderTest extends MongoTripodTestBase
{
    /** @var $indexer SearchIndexer */
    private $indexer;

    /** @var $indexer MongoSearchProvider */
    private $searchProvider;

    protected function setUp()
    {
        parent::setUp();

        $this->tripodTransactionLog = new \Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->tripod = new Tripod('CBD_testing', 'tripod_php_testing');
        $this->indexer = new SearchIndexer($this->tripod);
        $this->searchProvider = new MongoSearchProvider($this->tripod);
        $this->getTripodCollection($this->tripod)->drop();

        $this->loadBaseSearchDataViaTripod();

        foreach(\Tripod\Mongo\Config::getInstance()->getCollectionsForSearch($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop();
        }


        // index all the documents
        $cursor = $this->getTripodCollection($this->tripod)
            ->find(array("rdf:type.u"=>array('$in'=>array("bibo:Book"))),array('_id'=>1,'rdf:type'=>1));//->limit(20);
        foreach($cursor as $result)
        {
            $t = array();
            if(isset($result['rdf:type']['u'])){
                $t[] = $result['rdf:type']['u'];
            } else {
                foreach($result['rdf:type'] as $_t){
                    $t[] = $_t['u'];
                }
            }
            $this->indexer->generateAndIndexSearchDocuments($result['_id']['r'], $result['_id']['c'], $this->tripod->getPodName());
        }
    }

    public function testSearchIndexing() {


        // assert that there are only 12 based on the data we loaded into tripod
        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(12, $actualSearchDocumentCount, "Should have generated 12 search documents based on searchData.json");

        // define the expected search documents, this is what each of them should look like
        $expectedSearchDocs = array(
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc1","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"Physics for Engineers and Scientists", "link"=>"http://talisaspire.com/resources/doc1", "author"=>"Sayid Jarrah" ),
                "search_terms"=>array( "physics for engineers and scientists", "physics", "science", "sayid jarrah"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc1","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/1", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc2","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"Astrophysics for Beginners", "link"=>"http://talisaspire.com/resources/doc2", "author"=>"Sayid Jarrah" ),
                "search_terms"=>array( "astrophysics for beginners", "astrophysics", "science", "sayid jarrah"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc2","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/1", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc3","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"Fundementals of Heat and Mass Transfers", "link"=>"http://talisaspire.com/resources/doc3", "author"=>"Sayid Jarrah" ),
                "search_terms"=>array( "fundementals of heat and mass transfers", "engineering", "science", "sayid jarrah"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc3","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/1", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc4","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"Paradise Lost", "link"=>"http://talisaspire.com/resources/doc4", "author"=>"John Locke" ),
                "search_terms"=>array( "paradise lost", "english literature", "poetry", "classics", "john locke"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc4","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/2", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc5","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"The Divine Comedy", "link"=>"http://talisaspire.com/resources/doc5", "author"=>"John Locke" ),
                "search_terms"=>array( "the divine comedy", "dante's the divine comedy", "english literature", "poetry", "classics", "john locke"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc5","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/2", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc6","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"A Light in the Attic", "link"=>"http://talisaspire.com/resources/doc6", "author"=>"John Locke" ),
                "search_terms"=>array( "a light in the attic", "english literature", "poetry", "john locke"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc6","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/2", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc7","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"Revolting Rhymes", "link"=>"http://talisaspire.com/resources/doc7", "author"=>"John Locke" ),
                "search_terms"=>array( "revolting rhymes", "poetry", "john locke"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc7","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/2", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc8","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"The Owl and the Pussycat", "link"=>"http://talisaspire.com/resources/doc8", "author"=>"John Locke" ),
                "search_terms"=>array( "the owl and the pussycat", "poetry", "john locke"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc8","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/2", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc9","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"When We Were Very Young", "link"=>"http://talisaspire.com/resources/doc9", "author"=>"John Locke" ),
                "search_terms"=>array( "when we were very young", "poetry", "john locke"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc9","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/2", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc10","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"Love That Dog", "link"=>"http://talisaspire.com/resources/doc10", "author"=>"Jack Shephard" ),
                "search_terms"=>array( "love that dog", "poetry", "jack shephard"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc10","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/3", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc11","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"Where the Sidewalk Ends", "link"=>"http://talisaspire.com/resources/doc11", "author"=>"Jack Shephard" ),
                "search_terms"=>array( "where the sidewalk ends", "poetry", "jack shephard"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc11","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/3", "c" => "http://talisaspire.com/" )
                )
            ),
            array(
                "_id" => array("r"=>"http://talisaspire.com/resources/doc12","c"=>"http://talisaspire.com/","type"=>"i_search_resource"),
                "result" => array("title"=>"The Rime of the Ancient Mariner", "link"=>"http://talisaspire.com/resources/doc12", "author"=>"Jack Shephard" ),
                "search_terms"=>array( "the rime of the ancient mariner", "poetry", "jack shephard"),
                "_impactIndex"=>array(
                    array("r"=>"http://talisaspire.com/resources/doc12","c"=>"http://talisaspire.com/" ),
                    array("r" => "http://talisaspire.com/authors/3", "c" => "http://talisaspire.com/" )
                )
            )
        );

        // loop through every expected document and assert that it exists, and that each property matches the value we defined above.
        $searchCollection = \Tripod\Mongo\Config::getInstance()->getCollectionForSearchDocument($this->tripod->getStoreName(), 'i_search_resource');
        foreach($expectedSearchDocs as $expectedSearchDoc){
            $this->assertDocumentExists($expectedSearchDoc["_id"], $searchCollection);
            $this->assertDocumentHasProperty($expectedSearchDoc["_id"], "result", $expectedSearchDoc["result"], $searchCollection);
            $this->assertDocumentHasProperty($expectedSearchDoc["_id"], "search_terms", $expectedSearchDoc["search_terms"], $searchCollection);
            $this->assertDocumentHasProperty($expectedSearchDoc["_id"], "_impactIndex", $expectedSearchDoc["_impactIndex"], $searchCollection);
        }
    }

    public function testSearchIndexingRemovesDocWhenTypeHasNoCorrespondingSearchdocSpec()
    {
        // update a document
        $id = array('_id.r'=>'http://talisaspire.com/resources/doc1');
        $this->getTripodCollection($this->tripod)
            ->update($id, array('$set'=>array("rdf:type"=>array("u"=>"bibo:Article"))));

        // reindex
        $this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());

        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(11, $actualSearchDocumentCount, "Should only be 11 search documents now that one of them has had its type changed with no corresponding search doc spec");


        foreach(\Tripod\Mongo\Config::getInstance()->getCollectionsForSearch('tripod_php_testing') as $collection)
        {
            $this->assertNull(
                $collection->findOne(array("_id.r"=>"http://talisaspire.com/resources/doc1")),
                "No search document should be found, because it should have been deleted"
            );
        }

    }

    public function testSearchIndexingGeneratesNewDocForChangedTypeThatHasACorrespondingSearchdocSpec()
    {
        // update a document
        $id = array('_id.r'=>'http://talisaspire.com/resources/doc1');

        $newData = array(
            "rdf:type"=>array("u"=>"resourcelist:List"),
            "spec:name"=>array("l"=>"my list title"),
            "resourcelists:description"=>array("l"=>"foo bar baz"),

        );
        $this->getTripodCollection($this->tripod)->update($id, array('$set'=> $newData));

        // reindex
        $this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());

        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(12, $actualSearchDocumentCount, "Should only be 12 search documents");

        foreach(\Tripod\Mongo\Config::getInstance()->getCollectionsForSearch('tripod_php_testing') as $collection)
        {
            $result = $collection->findOne(array("_id.r"=>"http://talisaspire.com/resources/doc1"));
            if($result)
            {
                break;
            }
        }
        $this->assertEquals($result['_id'], array(
            'r'=>'http://talisaspire.com/resources/doc1',
            'c'=>'http://talisaspire.com/',
            'type'=>'i_search_list'
        ));
    }

    public function testSearchIndexingGeneratesTwoDocumentsForGivenResourceTheDeletesOneAfterFurtherUpdate()
    {
        // update a document
        $id = array('_id.r'=>'http://talisaspire.com/resources/doc1');

        $newData = array(
            "rdf:type"=>array(array("u"=>"resourcelist:List"),array("u"=>"bibo:Book")),
            "spec:name"=>array("l"=>"my list title"),
            "resourcelist:description"=>array("l"=>"foo bar baz"),
        );
        $this->getTripodCollection($this->tripod)->update($id, array('$set'=> $newData));

        // reindex
        $this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());

        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(13, $actualSearchDocumentCount, "Should only be 13 search documents");

        $results = array();
        // We don't know where exactly these might have stored
        foreach(\Tripod\Mongo\Config::getInstance()->getCollectionsForSearch('tripod_php_testing') as $collection)
        {
            foreach($collection->find(array("_id.r"=>"http://talisaspire.com/resources/doc1")) as $result)
            {
                if(!in_array($result, $results))
                {
                    $results[] = $result;
                }
            }
        }

        $this->assertEquals(2, count($results));
        $expected = array(
            array
            (
                'r'=>'http://talisaspire.com/resources/doc1',
                'c'=>'http://talisaspire.com/',
                'type'=>'i_search_list'
            ),
            array
            (
                'r'=>'http://talisaspire.com/resources/doc1',
                'c'=>'http://talisaspire.com/',
                'type'=>'i_search_resource'
            )
        );
        foreach($results as $result){
            $this->assertTrue(in_array(
                $result['_id'], $expected
            ));
        }

        // now update it again removing the resourcelist:List type
        $newData = array(
            "rdf:type"=>array("u"=>"bibo:Book")
        );
        $this->getTripodCollection($this->tripod)->update($id, array('$set'=> $newData));

        // reindex
        $this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());

        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

        $this->assertEquals(12, $actualSearchDocumentCount, "Should only be 12 search documents");

        $results = array();
        // We don't know where exactly these might have stored
        foreach(\Tripod\Mongo\Config::getInstance()->getCollectionsForSearch('tripod_php_testing') as $collection)
        {
            foreach($collection->find(array("_id.r"=>"http://talisaspire.com/resources/doc1")) as $result)
            {
                if(!in_array($result, $results))
                {
                    $results[] = $result;
                }
            }
        }

        $this->assertEquals(1, count($results));

        $result = array_pop($results);
        $this->assertEquals($result['_id'], array(
            'r'=>'http://talisaspire.com/resources/doc1',
            'c'=>'http://talisaspire.com/',
            'type'=>'i_search_resource'
        ));

    }

    public function testSearchThrowsExceptionIfNoQuery()
    {
        $this->setExpectedException("\Tripod\Exceptions\SearchException","You must specify a query");
        $this->searchProvider->search("", "i_search_resource",  array("search_terms"), array("result"), 3, 0);
    }

    public function testSearchThrowsExceptionIfNoType()
    {
        $this->setExpectedException("\Tripod\Exceptions\SearchException","You must specify the search document type to restrict the query to");
        $this->searchProvider->search("poetry", "",  array("search_terms"), array("result"), 3, 0);
    }

    public function testSearchThrowsExceptionIfSearchIndicesEmpty()
    {
        $this->setExpectedException("\Tripod\Exceptions\SearchException","You must specify at least one index from the search document specification to query against");
        $this->searchProvider->search("poetry", "i_search_resource",  array(), array("result"), 3, 0);
    }

    public function testSearchThrowsExceptionIfFieldsToReturnEmpty()
    {
        $this->setExpectedException("\Tripod\Exceptions\SearchException","You must specify at least one field from the search document specification to return");
        $this->searchProvider->search("poetry", "i_search_resource",  array("search_terms"), array(), 3, 0);
    }


    public function testSearchThrowsExceptionIfLimitIsNegative()
    {
        $this->setExpectedException("\Tripod\Exceptions\SearchException","Value for limit must be a positive number");
        $this->searchProvider->search("poetry", "i_search_resource",  array("search_terms"), array("result"), -3, 0);
    }

    public function testSearchThrowsExceptionIfOffsetIsNegative()
    {
        $this->setExpectedException("\Tripod\Exceptions\SearchException","Value for offset must be a positive number");
        $this->searchProvider->search("poetry", "i_search_resource",  array("search_terms"), array("result"), 3, -1);
    }

    public function testSearchLimitAndOffset()
    {
        $results = $this->searchProvider->search("poetry", "i_search_resource", array("search_terms"), array("result"), 3, 0);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(0, $results['head']['offset']);

        $firstResult    = $results['results'][0];
        $secondResult   = $results['results'][1];
        $thirdResult    = $results['results'][2];

        $results2 = $this->searchProvider->search("poetry", "i_search_resource", array("search_terms"), array("result"), 3, 1);
        $this->assertEquals(9, $results2['head']['count']);
        $this->assertEquals(3, $results2['head']['limit']);
        $this->assertEquals(1, $results2['head']['offset']);

        $this->assertFalse(in_array($firstResult, $results2['results']));
        $this->assertEquals($secondResult, $results2['results'][0]);
        $this->assertEquals($thirdResult, $results2['results'][1]);
    }

    public function testSearchSingleIndex()
    {
        // simple search
        $results = $this->searchProvider->search("john locke poetry", "i_search_resource", array("search_terms"), array("result"), 3, 0);
        $this->assertEquals(6, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(3, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals("john locke poetry",$results['head']["query"]);
        $this->assertEquals(array("john","locke","poetry"), $results['head']["query_terms_used"]);
        $this->assertArrayHasKey("duration", $results['head']);

        $expectedResults = array(
            array("title"=>"Paradise Lost" , "link"=>"http://talisaspire.com/resources/doc4" , "author"=>"John Locke"),
            array("title"=>"The Divine Comedy" , "link"=>"http://talisaspire.com/resources/doc5" , "author"=>"John Locke"),
            array("title"=>"A Light in the Attic" , "link"=>"http://talisaspire.com/resources/doc6" , "author"=>"John Locke"),
        );

        $this->assertEquals($expectedResults, $results['results']);

        // search with some stop words
        $results = $this->searchProvider->search("the owl and the pussycat", "i_search_resource", array("search_terms"), array("result"), 3, 0);
        $this->assertEquals(1, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(1, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals("the owl and the pussycat",$results['head']["query"]);
        $this->assertEquals(array("owl","pussycat"), $results['head']["query_terms_used"]);
        $this->assertArrayHasKey("duration", $results['head']);

        // search returns no results
        $results = $this->searchProvider->search("october", "i_search_resource", array("search_terms"), array("result"), 3, 0);
        $this->assertEquals(0, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(0, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals("october",$results['head']["query"]);
        $this->assertEquals(array("october"), $results['head']["query_terms_used"]);
        $this->assertArrayHasKey("duration", $results['head']);
        $this->assertEquals(array(), $results['results']);

        // search single index but return multiple fields
        $results = $this->searchProvider->search("john locke poetry", "i_search_resource", array("search_terms"), array("result","rdftype"), 3, 0);
        $this->assertEquals(6, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(3, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals("john locke poetry",$results['head']["query"]);
        $this->assertEquals(array("john","locke","poetry"), $results['head']["query_terms_used"]);
        $this->assertArrayHasKey("duration", $results['head']);

        $expectedResults = array(
            array("result"=>array("title"=>"Paradise Lost" , "link"=>"http://talisaspire.com/resources/doc4" , "author"=>"John Locke"), "rdftype"=>"bibo:Book"),
            array("result"=>array("title"=>"The Divine Comedy" , "link"=>"http://talisaspire.com/resources/doc5" , "author"=>"John Locke"),"rdftype"=>"bibo:Book"),
            array("result"=>array("title"=>"A Light in the Attic" , "link"=>"http://talisaspire.com/resources/doc6" , "author"=>"John Locke"),"rdftype"=>"bibo:Book")
        );
        $this->assertEquals($expectedResults, $results['results']);

    }

    public function testSearchMultipleIndices()
    {
        $results = $this->searchProvider->search("bibo:Book", "i_search_resource", array("search_terms","other_terms"), array("result","rdftype"), 3, 0);
        $this->assertEquals(12, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(3, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals("bibo:Book",$results['head']["query"]);
        $this->assertEquals(array("bibo:book"), $results['head']["query_terms_used"]);
        $this->assertArrayHasKey("duration", $results['head']);

        $expectedResults = array(
            array("result"=>array("title"=>"Physics for Engineers and Scientists" , "link"=>"http://talisaspire.com/resources/doc1" , "author"=>"Sayid Jarrah"), "rdftype"=>"bibo:Book"),
            array("result"=>array("title"=>"Astrophysics for Beginners" , "link"=>"http://talisaspire.com/resources/doc2" , "author"=>"Sayid Jarrah"), "rdftype"=>"bibo:Book"),
            array("result"=>array("title"=>"Fundementals of Heat and Mass Transfers" , "link"=>"http://talisaspire.com/resources/doc3" , "author"=>"Sayid Jarrah"), "rdftype"=>"bibo:Book")
        );

        $this->assertEquals($expectedResults, $results['results']);
    }

    public function testSearchWorksDirectlyFromTripod()
    {
       $results =  $this->tripod->search(array(
            'q'=>'john locke poetry',
            'type'=>'i_search_resource',
            'indices'=>array("search_terms"),
            "fields"=>array("result"),
            "limit"=>3,
            "offset"=>0
        ));

        $this->assertEquals(6, $results['head']['count']);
        $this->assertEquals(3, $results['head']['limit']);
        $this->assertEquals(3, count($results['results']));
        $this->assertEquals(0, $results['head']['offset']);
        $this->assertEquals("john locke poetry",$results['head']["query"]);
        $this->assertEquals(array("john","locke","poetry"), $results['head']["query_terms_used"]);
        $this->assertArrayHasKey("duration", $results['head']);

        $expectedResults = array(
            array("title"=>"Paradise Lost" , "link"=>"http://talisaspire.com/resources/doc4" , "author"=>"John Locke"),
            array("title"=>"The Divine Comedy" , "link"=>"http://talisaspire.com/resources/doc5" , "author"=>"John Locke"),
            array("title"=>"A Light in the Attic" , "link"=>"http://talisaspire.com/resources/doc6" , "author"=>"John Locke"),
        );

        $this->assertEquals($expectedResults, $results['results']);

    }
    
    public function testDeleteSearchDocumentsByTypeIdThrowsExceptionForInvalidType()
    {
    	$mockSearchProvider = $this->getMock("\Tripod\Mongo\MongoSearchProvider", array('getSearchDocumentSpecification'), array($this->tripod));
    	$mockSearchProvider->expects($this->once())
    						->method('getSearchDocumentSpecification')
    						->with('i_some_type')
    						->will($this->returnValue(null));
    	
    	$this->setExpectedException("Exception","Could not find a search specification for i_some_type");
    	$mockSearchProvider->deleteSearchDocumentsByTypeId('i_some_type');
    }
    
    public function testDeleteSearchDocumentsByTypeIdDeletesNothingWhenNoMatchFound()
    {
    	// first, assert that there are only 12 based on the data we loaded into tripod
        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

    	$this->assertEquals(12, $actualSearchDocumentCount, "Should have generated 12 search documents based on searchData.json");

        /** @var MongoSearchProvider|PHPUnit_Framework_MockObject_MockObject $mockSearchProvider */
    	$mockSearchProvider = $this->getMock("\Tripod\Mongo\MongoSearchProvider", array('getSearchDocumentSpecification'), array($this->tripod));
    	$mockSearchProvider->expects($this->once())
				    	->method('getSearchDocumentSpecification')
				    	->with('i_some_type')
				    	->will($this->returnValue(array('i_some_type' => array())));
        try
        {
    	    $mockSearchProvider->deleteSearchDocumentsByTypeId('i_some_type');
        }
        catch(\Tripod\Exceptions\ConfigException $e)
        {
            $this->assertEquals("Search document id 'i_some_type' not in configuration for store 'tripod_php_testing'", $e->getMessage());
        }
    	
    	//search document count should remain same, because we expect that there was nothing to delete 
        $newSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

    	$this->assertEquals(12, $newSearchDocumentCount, "Should have generated 12 search documents, because there was no match to remove");
    }
    
    public function testDeleteSearchDocumentsByTypeIdDeleteAllMatchingDocuments()
    {
    	// first, assert that there are only 12 based on the data we loaded into tripod
        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

    	$this->assertEquals(12, $actualSearchDocumentCount, "Should have generated 12 search documents based on searchData.json");
    	 
    	$mockSearchProvider = $this->getMock("\Tripod\Mongo\MongoSearchProvider", array('getSearchDocumentSpecification'), array($this->tripod));
    	$mockSearchProvider->expects($this->once())
    					->method('getSearchDocumentSpecification')
				    	->with('i_search_resource')
				    	->will($this->returnValue(array('i_search_resource' => array())));
    
    	$mockSearchProvider->deleteSearchDocumentsByTypeId('i_search_resource');
    	 
    	//search document count should be 0, because we expect that everything should be deleted
        $newSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

    	$this->assertEquals(0, $newSearchDocumentCount, "Should have 0 search documents after removing all matching documents");
    }
    
    public function testDeleteSearchDocumentsByTypeIdDoNotDeleteNonMatchingDocuments()
    {
    	// first, assert that there are only 12 based on the data we loaded into tripod
        $actualSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

    	$this->assertEquals(12, $actualSearchDocumentCount, "Should have generated 12 search documents based on searchData.json");

    	$id = array('_id.r'=>'http://talisaspire.com/resources/doc1');
    	
    	$newData = array(
    			"rdf:type"=>array(array("u"=>"resourcelist:List"),array("u"=>"bibo:Book")),
    			"spec:name"=>array("l"=>"my list title"),
    			"resourcelist:description"=>array("l"=>"foo bar baz"),
    	);
    	$this->getTripodCollection($this->tripod)->update($id, array('$set'=> $newData));
    	
    	// reindex
    	$this->indexer->generateAndIndexSearchDocuments('http://talisaspire.com/resources/doc1', 'http://talisaspire.com/', $this->tripod->getPodName());
    	 
    	//assert that there are now 13 documents after adding new document to collection
        $updatedSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

    	$this->assertEquals(13, $updatedSearchDocumentCount, "Should have generated 13 search documents after adding a new document to collection");
    
    	$mockSearchProvider = $this->getMock("\Tripod\Mongo\MongoSearchProvider", array('getSearchDocumentSpecification'), array($this->tripod));
    	$mockSearchProvider->expects($this->once())
				    	->method('getSearchDocumentSpecification')
				    	->with('i_search_resource')
				    	->will($this->returnValue(array('i_search_resource' => array())));
    
    	$mockSearchProvider->deleteSearchDocumentsByTypeId('i_search_resource');
    
    	//search document count should be 1, since there is one document not matching the type id provided for delete
        $newSearchDocumentCount = $this->getCountForSearchSpecs($this->tripod);

    	$this->assertEquals(1, $newSearchDocumentCount, "Should have 1 search documents since there is one search document with 'i_search_list' type that does not match delete type.");
    }

    /**
     * @param Tripod $tripod
     * @param array $specs
     * @return int
     */
    protected function getCountForSearchSpecs(Tripod $tripod, $specs = array())
    {
        $count = 0;
        if(empty($specs))
        {
            $specs = \Tripod\Mongo\Config::getInstance()->getSearchDocumentSpecifications($tripod->getStoreName(), null, true);
        }

        foreach($specs as $spec)
        {
            $count += \Tripod\Mongo\Config::getInstance()->getCollectionForSearchDocument($tripod->getStoreName(), $spec)->count(array('_id.type'=>$spec));
        }
        return $count;
    }
}