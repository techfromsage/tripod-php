<?php

class TestConfigGenerator extends Tripod\Mongo\Config
{
    protected $fileName;

    private function __construct() {}

    public function serialize()
    {
        return ['class' => get_class($this), 'filename' => $this->fileName];
    }

    public static function deserialize(array $config)
    {
        $instance = new self();
        $instance->fileName = $config['filename'];
        $cfg = json_decode(file_get_contents($config['filename']), true);
        $instance->loadConfig($cfg);
        return $instance;
    }
}
