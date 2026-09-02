<?php

use Tripod\Exceptions\ConfigException;
use Tripod\Mongo\Config;
use Tripod\Mongo\IConfigInstance;

class TestConfigGenerator extends Config
{
    private string $fileName;

    private function __construct() {}

    public function serialize(): array
    {
        return ['class' => get_class($this), 'filename' => $this->fileName];
    }

    public static function deserialize(array $config): IConfigInstance
    {
        $instance = new self();
        if (!isset($config['filename']) || !is_string($config['filename'])) {
            throw new ConfigException('TestConfigGenerator requires a filename');
        }
        $instance->fileName = $config['filename'];

        $cfg = json_decode((string) file_get_contents($config['filename']), true);
        $instance->loadConfig(is_array($cfg) ? $cfg : []);

        return $instance;
    }
}
