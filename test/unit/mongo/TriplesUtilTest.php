<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoTripodConfig.class.php';
require_once 'src/mongo/util/TriplesUtil.class.php';

class TriplesUtilTest extends MongoTripodTestBase
{
    protected function setUp()
    {
        parent::setup();
    }

    public function testGetTArrayAbout()
    {
        $tu = new TriplesUtil();
        $triples = array();

        $triples[] = "<http://serials.talisaspire.com/issn/0893-0465> <http://xmlns.com/foaf/0.1/page> <http://www.ingentaconnect.com/content/bpl/ciso> . ";
        $triples[] = "<http://serials.talisaspire.com/issn/0893-0465> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Journal> . ";
        $triples[] = "<http://serials.talisaspire.com/issn/0893-0465> <http://xmlns.com/foaf/0.1/page> <http://onlinelibrary.wiley.com/journal/10.1111/(ISSN)1548-744X> . ";
        $triples[] = "<http://serials.talisaspire.com/issn/0893-0465> <http://purl.org/ontology/bibo/eissn> \"1548-774X\" . ";

        $expectedDoc = array(
            '_id'=>array('r'=>'http://serials.talisaspire.com/issn/0893-0465',"c"=>"http://talisaspire.com/"),
            'foaf:page'=>array(
                array(
                    'u'=>'http://www.ingentaconnect.com/content/bpl/ciso'),
                array(
                    'u'=>'http://onlinelibrary.wiley.com/journal/10.1111/(ISSN)1548-744X')
            ),
            'rdf:type'=>array(
                'u'=>'bibo:Journal'
            ),
            'bibo:eissn'=>array(
                'l'=>'1548-774X'
            )
        );
        $this->assertEquals($expectedDoc,$tu->getTArrayAbout('http://serials.talisaspire.com/issn/0893-0465',$triples,'http://talisaspire.com/'));
    }

    //todo: add triples test
}