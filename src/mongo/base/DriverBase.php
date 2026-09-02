<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\ReadPreference;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Tripod\ITripodStat;
use Tripod\LoggerTrait;
use Tripod\Mongo\Composites\Views;
use Tripod\StatsD;
use Tripod\Timer;
use Tripod\TripodStatFactory;

abstract class DriverBase
{
    use LoggerTrait;

    public static ?LoggerInterface $logger = null;

    protected string $storeName;

    protected string $podName;

    protected ?string $defaultContext = null;

    protected ?ITripodStat $stat = null;

    protected array $statsConfig = [];

    protected ?Database $db = null;

    protected ?Collection $collection = null;

    protected string $readPreference = ReadPreference::PRIMARY_PREFERRED;

    protected Labeller $labeller;

    protected IConfigInstance $config;

    /**
     * @codeCoverageIgnore
     */
    public static function getLogger(): LoggerInterface
    {
        if (self::$logger == null) {
            self::$logger = new NullLogger();
        }

        return self::$logger;
    }

    public function getStat(): ITripodStat
    {
        $stat = $this->stat;
        if ($stat == null) {
            $stat = $this->getStatFromStatFactory();
            $this->setStat($stat);
        }

        return $stat;
    }

    public function setStat(ITripodStat $stat): void
    {
        // TODO: how do we decouple this and still allow StatsD to know which db we're using?
        if ($stat instanceof StatsD && isset($this->storeName)) {
            $stat->setPivotValue($this->getStoreName());
        }

        $this->stat = $stat;
    }

    /**
     * Returns stat object config.
     */
    public function getStatsConfig(): array
    {
        return $this->getStat()->getConfig();
    }

    public function getStoreName(): string
    {
        return $this->storeName;
    }

    public function getPodName(): string
    {
        return $this->podName;
    }

    /**
     * For mocking out the creation of stat objects.
     */
    protected function getStatFromStatFactory(): ITripodStat
    {
        return TripodStatFactory::create($this->statsConfig);
    }

    protected function getContextAlias(?string $context = null): string
    {
        $contextAlias = $this->labeller->uri_to_alias((empty($context)) ? $this->defaultContext : $context);

        return (empty($contextAlias)) ? $this->getConfigInstance()->getDefaultContextAlias() : $contextAlias;
    }

    protected function fetchGraph(array $query, string $type, ?Collection $collection = null, ?array $includeProperties = [], int $cursorSize = 101): MongoGraph
    {
        $graph = new MongoGraph();

        $t = new Timer();
        $t->start();

        if ($collection == null) {
            $collection = $this->getCollection();
        }

        $collectionName = $collection->getCollectionName();

        if (empty($includeProperties)) {
            $cursor = $collection->find($query);
        } else {
            $fields = [];
            foreach ($includeProperties as $property) {
                $fields[$this->labeller->uri_to_alias($property)] = true;
            }

            $cursor = $collection->find($query, [
                'projection' => $fields,
                'batchSize' => $cursorSize,
            ]);
        }

        $retries = 1;
        while (true) {
            try {
                $exception = null;

                foreach ($cursor as $result) {
                    // handle MONGO_VIEWS that have expired due to ttl. These are expired
                    // on read (lazily) rather than on write
                    if ($this instanceof Views && $type == MONGO_VIEW && isset($result['value'][_EXPIRES])) {
                        // if expires < current date, regenerate view..
                        $expires = $result['value'][_EXPIRES];
                        $currentDate = DateUtil::getMongoDate();
                        if ($expires < $currentDate) {
                            // regenerate!
                            $this->generateView($result[_ID_KEY][_ID_TYPE], $result[_ID_KEY][_ID_RESOURCE]);
                        }
                    }

                    $graph->add_tripod_array($result);
                }

                // Cursor exhausted successfully, break out of retry loop.
                break;
            } catch (\Exception $e) {
                $exception = $e;

                if ($retries >= Config::CONNECTION_RETRIES) {
                    break;
                }

                self::getLogger()->error('CursorException attempt ' . $retries . '. Retrying...:' . $e->getMessage());
                sleep(1);
                $retries++;
            }
        }

        if (isset($exception)) {
            self::getLogger()->error('CursorException failed after ' . $retries . ' attempts (MAX:' . Config::CONNECTION_RETRIES . '): ' . $exception->getMessage());

            throw $exception;
        }

        $t->stop();
        $this->timingLog($type, ['duration' => $t->result(), 'query' => $query, 'collection' => $collectionName]);
        if ($type == MONGO_VIEW) {
            if (array_key_exists('_id.type', $query)) {
                $this->getStat()->timer(sprintf('%s.%s', $type, $query['_id.type']), $t->result());
            } elseif (array_key_exists(_ID_KEY, $query) && array_key_exists(_ID_TYPE, $query[_ID_KEY])) {
                $this->getStat()->timer(sprintf('%s.%s', $type, $query[_ID_KEY][_ID_TYPE]), $t->result());
            }
        } else {
            $this->getStat()->timer(sprintf('%s.%s', $type, $collectionName), $t->result());
        }

        return $graph;
    }

    /**
     * Expands an RDF sequence into proper tripod join clauses.
     */
    protected function expandSequence(array &$joins, array $source): void
    {
        if (!empty($joins) && isset($joins['followSequence'])) {
            // add any rdf:_x style properties in the source to the joins array,
            // up to rdf:_1000 (unless a max is specified in the spec)
            $max = $joins['followSequence']['maxJoins'] ?? 1000;
            for ($i = 0; $i < $max; $i++) {
                $r = 'rdf:_' . ($i + 1);

                if (isset($source[$r])) {
                    $joins[$r] = [];
                    foreach ($joins['followSequence'] as $k => $v) {
                        if ($k != 'maxJoins') {
                            $joins[$r][$k] = $joins['followSequence'][$k];
                        } else {
                            continue;
                        }
                    }
                }
            }

            unset($joins['followSequence']);
        }
    }

    /**
     * Adds an _id object (or array of _id objects) to the target document's impact index.
     *
     * @throws \InvalidArgumentException
     */
    protected function addIdToImpactIndex(array $id, array &$target, bool $buildImpactIndex = true): void
    {
        if ($buildImpactIndex) {
            if (isset($id[_ID_RESOURCE])) {
                // Ensure that our id is curie'd
                $id[_ID_RESOURCE] = $this->labeller->uri_to_alias($id[_ID_RESOURCE]);
                if (!isset($target[_IMPACT_INDEX])) {
                    $target[_IMPACT_INDEX] = [];
                }

                if (!in_array($id, $target[_IMPACT_INDEX])) {
                    $target[_IMPACT_INDEX][] = $id;
                }
            } else { // Assume this is an array of ids
                foreach ($id as $i) {
                    if (!isset($i[_ID_RESOURCE])) {
                        throw new \InvalidArgumentException('Invalid id format');
                    }

                    $this->addIdToImpactIndex($i, $target);
                }
            }
        }
    }

    /**
     * For mocking.
     */
    protected function getConfigInstance(): IConfigInstance
    {
        return \Tripod\Config::getInstance();
    }

    protected function getDatabase(): Database
    {
        if ($this->db === null) {
            $this->db = $this->config->getDatabase(
                $this->storeName,
                $this->config->getDataSourceForPod($this->storeName, $this->podName),
                $this->readPreference
            );
        }

        return $this->db;
    }

    protected function getCollection(): Collection
    {
        if ($this->collection === null) {
            $this->collection = $this->getDatabase()->selectCollection($this->podName);
        }

        return $this->collection;
    }
}
