<?php

require_once TRIPOD_DIR.'/mongo/queue/MongoTripodQueue.class';

class TestTripodQueue extends MongoTripodQueue {

    protected $template = null;

    public function __construct($stat=null, $template) {
        parent::__construct($stat);
        $this->template = $template;
    }

    protected function getMongoTripod($data) {
        setConfig($data);
        parent::getMongoTripod($data);
    }

    public function setConfig($opts)
    {
        $c = $this->template;

        $c = str_replace('#SHORT_CODE#',$opts["tenantShortCode"],$c);
        $c = str_replace('#BASE_URI#',$opts["baseUri"],$c);

        $config = json_decode($c,true);
        if ($config==null) throw new Exception("Problem assembling config, template: \n$c");

        MongoTripodConfig::setConfig($config);
    }
}

?>