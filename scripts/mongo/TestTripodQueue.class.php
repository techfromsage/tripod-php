<?php

require_once TRIPOD_DIR.'/mongo/queue/MongoTripodQueue.class.php';

class TestTripodQueue extends MongoTripodQueue {

    protected $mapDatabaseToBaseUri = array(
        "kent" => "http://resourcelists.kent.ac.uk",
        "mmu"  => "http://lists.lib.mmu.ac.uk",
        "ntu"  => "http://resourcelists.ntu.ac.uk",
        "sta"  => "http://resourcelists.st-andrews.ac.uk",
        "worc" => "http://readinglists.worcs.ac.uk"
    );

    protected $template = null;

    public function __construct($stat=null, $template) {
        parent::__construct($stat);
        $this->template = $template;
    }

    protected function getMongoTripod($data) {
        $this->setConfig($data);
        return parent::getMongoTripod($data);
    }

    public function setConfig($opts)
    {
        if (!isset($opts["database"])) throw new Exception("Problem assembling config, template: Missing database: " . serialize($opts));
        $baseUrl = $this->databaseToBaseUri($opts["database"]);

        $c = $this->template;
        $c = str_replace('#SHORT_CODE#',$opts["database"],$c);
        $c = str_replace('#BASE_URI#',$baseUrl,$c);

        $config = json_decode($c,true);
        if ($config==null) throw new Exception("Problem assembling config, template: \n$c");

        MongoTripodConfig::setConfig($config);
    }

    protected function databaseToBaseUri($database) {
        return $this->mapDatabaseToBaseUri[$database];
    }
}

?>