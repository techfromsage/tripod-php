<?php

declare(strict_types=1);

namespace Tripod;

class StatsD implements ITripodStat
{
    private string $host = '';

    private int $port = 0;

    private ?string $prefix = null;

    private ?string $pivotValue = null;

    /**
     * @param int|string $port
     */
    public function __construct(string $host, $port, ?string $prefix = '')
    {
        $this->setHost($host);
        $this->setPort($port);
        $this->setPrefix($prefix);
    }

    public function increment(string $operation, int $inc = 1): void
    {
        $this->send(
            $this->generateStatData($operation, $inc . '|c')
        );
    }

    /**
     * @param float|int $duration
     */
    public function timer(string $operation, $duration): void
    {
        $this->send(
            $this->generateStatData($operation, ['1|c', $duration . '|ms'])
        );
    }

    /**
     * Record an arbitrary value.
     */
    public function gauge(string $operation, string $value): void
    {
        $this->send(
            $this->generateStatData($operation, $value . '|g')
        );
    }

    /**
     * @return array{class: class-string<StatsD>, config: array{host: string, port: int, prefix: string|null}}
     */
    public function getConfig(): array
    {
        return [
            'class' => get_class($this),
            'config' => [
                'host' => $this->host,
                'port' => $this->port,
                'prefix' => $this->prefix,
            ],
        ];
    }

    /**
     * @return self
     */
    public static function createFromConfig(array $config)
    {
        if (isset($config['config'])) {
            $config = $config['config'];
        }

        $host = ($config['host'] ?? null);
        $port = ($config['port'] ?? null);
        $prefix = ($config['prefix'] ?? '');

        return new self($host, $port, $prefix);
    }

    public function getPrefix(): ?string
    {
        return $this->prefix;
    }

    /**
     * @throws \InvalidArgumentException
     */
    public function setPrefix(?string $prefix): void
    {
        if ($this->isValidPathValue($prefix)) {
            $this->prefix = $prefix;
        } else {
            throw new \InvalidArgumentException('Invalid prefix supplied');
        }
    }

    public function getPort(): int
    {
        return $this->port;
    }

    /**
     * @param int|string $port
     */
    public function setPort($port): void
    {
        $this->port = (int) $port;
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function setHost(string $host): void
    {
        $this->host = $host;
    }

    public function getPivotValue(): ?string
    {
        return $this->pivotValue;
    }

    /**
     * @throws \InvalidArgumentException
     */
    public function setPivotValue(?string $pivotValue): void
    {
        if ($this->isValidPathValue($pivotValue)) {
            $this->pivotValue = $pivotValue;
        } else {
            throw new \InvalidArgumentException('Invalid pivot value supplied');
        }
    }

    /**
     * Sends the stat(s) using UDP protocol.
     */
    protected function send(array $data, int $sampleRate = 1): void
    {
        if (empty($this->host)) {
            return;
        }

        $sampledData = [];
        if ($sampleRate < 1) {
            foreach ($data as $stat => $value) {
                if ((mt_rand() / mt_getrandmax()) <= $sampleRate) {
                    $sampledData[$stat] = sprintf('%s|@%d', $value, $sampleRate);
                }
            }
        } else {
            $sampledData = $data;
        }

        if (empty($sampledData)) {
            return;
        }

        try {
            $fp = fsockopen('udp://' . $this->host, $this->port);
            if (!$fp) {
                return;
            }

            // make this a non blocking stream
            stream_set_blocking($fp, false);
            foreach ($sampledData as $stat => $value) {
                if (is_array($value)) {
                    foreach ($value as $v) {
                        fwrite($fp, sprintf('%s:%s', $stat, $v));
                    }
                } else {
                    fwrite($fp, sprintf('%s:%s', $stat, $value));
                }
            }

            fclose($fp);
        } catch (\Exception $e) {
        }
    }

    /**
     * This method combines the by database and aggregate stats to send to StatsD.  The return will look something list:
     * {
     *  "{prefix}.tripod.group_by_db.{storeName}.{stat}"=>"1|c",
     *  "{prefix}.tripod.{stat}"=>"1|c"
     * }
     *
     * @param array|string $value
     *
     * @return array An associative array of the grouped_by_database and aggregate stats
     */
    protected function generateStatData(string $operation, $value): array
    {
        $data = [];
        foreach ($this->getStatsPaths() as $path) {
            $data[$path . ('.' . $operation)] = $value;
        }

        return $data;
    }

    protected function getStatsPaths(): array
    {
        return array_filter([$this->getAggregateStatPath()]);
    }

    protected function getAggregateStatPath(): string
    {
        return empty($this->prefix) ? STAT_CLASS : $this->prefix . '.' . STAT_CLASS;
    }

    /**
     * StatsD paths cannot start with, end with, or have more than one consecutive '.'.
     */
    protected function isValidPathValue(?string $value): bool
    {
        return $value === null || preg_match('/(^\.)|(\.\.+)|(\.$)/', $value) === 0;
    }
}
