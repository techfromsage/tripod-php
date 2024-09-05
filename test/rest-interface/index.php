<?php

require 'vendor/autoload.php';
require 'vendor/talis/tripod-php/src/tripod.inc.php';
define('FORMAT_RDF_XML', 'rdfxml');
define('FORMAT_NTRIPLES', 'ntriples');
define('FORMAT_TURTLE', 'turtle');
define('FORMAT_RDF_JSON', 'rdfjson');
$app = new \Slim\Slim(array(
    'log.level' => \Slim\Log::DEBUG
));

$appConfig = json_decode(file_get_contents('./config/config.json'), true);

$tripodOptions = array();
if (isset($appConfig['tripod'])) {
    $tripodOptions = $appConfig['tripod'];
}

if (isset($appConfig['statsConfig'])) {
    $tripodOptions['statsConfig'] = $appConfig['statsConfig'];
}

$readRepeat = isset($appConfig['read-repeat']) ? (int)$appConfig['read-repeat'] : 0;

define('READ_REPEAT_NUM', $readRepeat);

$app->group('/1', function () use ($app, $tripodOptions) {
    $app->group('/:storeName', function () use ($app, $tripodOptions) {
        $app->get('/views/:viewId/:encodedFqUri', function ($storeName, $viewSpecId, $encodedFqUri) use ($app, $tripodOptions) {
            \Tripod\Config::setConfig(json_decode(file_get_contents('./config/tripod-config-'.$storeName .'.json'), true));
            $i = 0;
            do {
                $viewSpec = \Tripod\Config::getInstance()->getViewSpecification($storeName, $viewSpecId);
                $podName = isset($viewSpec['from']) ? $viewSpec['from'] : null;
                $tripodOptions['statsConfig'] = getStat($app, $tripodOptions);
                $tripod = new \Tripod\Mongo\Driver($podName, $storeName, $tripodOptions);
                $contentType = $app->request()->getMediaType();
                switch ($contentType) {
                    case 'application/rdf+xml':
                        $format = FORMAT_RDF_XML;
                        break;
                    case 'text/plain':
                        $format = FORMAT_NTRIPLES;
                        break;
                    case 'text/turtle':
                        $format = FORMAT_TURTLE;
                        break;
                    default:
                        $format = FORMAT_RDF_JSON;
                }
                $graph =  $tripod->getViewForResource(base64_decode($encodedFqUri), $viewSpecId);
            } while (++$i < READ_REPEAT_NUM);
            if ($graph->is_empty()) {
                $app->response()->setStatus(404);
            } else {
                $output = getFormattedGraph($graph, $format);
                $app->response()->headers()->set('Content-type', getContentType($format));
                echo $output;
            }
        });
        $app->group('/:podName', function () use ($app, $tripodOptions) {
            $app->group('/graph', function () use ($app, $tripodOptions) {
                $app->get('/:encodedFqUri', function ($storeName, $podName, $encodedFqUri) use ($app, $tripodOptions) {
                    \Tripod\Config::setConfig(json_decode(file_get_contents('./config/tripod-config-'.$storeName .'.json'), true));
                    $tripodOptions['statsConfig'] = getStat($app, $tripodOptions);

                    $contentType = $app->request()->getMediaType();
                    $i = 0;
                    do {
                        $tripod = new \Tripod\Mongo\Driver($podName, $storeName, $tripodOptions);

                        switch ($contentType) {
                            case 'application/rdf+xml':
                                $format = FORMAT_RDF_XML;
                                break;
                            case 'text/plain':
                                $format = FORMAT_NTRIPLES;
                                break;
                            case 'text/turtle':
                                $format = FORMAT_TURTLE;
                                break;
                            default:
                                $format = FORMAT_RDF_JSON;
                        }
                        $graph =  $tripod->describeResource(base64_decode($encodedFqUri));
                    } while (++$i < READ_REPEAT_NUM);

                    if ($graph->is_empty()) {
                        $app->response()->setStatus(404);
                    } else {
                        $output = getFormattedGraph($graph, $format);
                        $app->response()->headers()->set('Content-type', getContentType($format));
                        echo $output;
                    }

                });

                $app->delete('/:encodedFqUri', function ($storeName, $podName, $encodedFqUri) use ($app, $tripodOptions) {
                    $tripodOptions['statsConfig'] = getStat($app, $tripodOptions);
                    $tripod = new \Tripod\Mongo\Driver($podName, $storeName, $tripodOptions);
                    $oldGraph = $tripod->describeResource(base64_decode($encodedFqUri));
                    $tripod->saveChanges($oldGraph, new \Tripod\ExtendedGraph());
                });

                $app->post('/', function ($storeName, $podName) use ($app, $tripodOptions) {
                    $tripodOptions['statsConfig'] = getStat($app, $tripodOptions);
                    $tripod = new \Tripod\Mongo\Driver($podName, $storeName, $tripodOptions);
                    $rawGraphData = $app->request()->getBody();
                    $graph = new \Tripod\Mongo\MongoGraph();
                    $graph->add_rdf($rawGraphData);
                    $tripod->saveChanges(new \Tripod\ExtendedGraph(), $graph);
                });
            });

            $app->group('/change', function () use ($app, $tripodOptions) {
                $app->post('/', function ($storeName, $podName) use ($app, $tripodOptions) {
                    \Tripod\Config::setConfig(json_decode(file_get_contents('./config/tripod-config-'.$storeName .'.json'), true));
                    $app->response()->setStatus(500);
                    $tripodOptions['statsConfig'] = getStat($app, $tripodOptions);
                    $tripod = new \Tripod\Mongo\Driver($podName, $storeName, $tripodOptions);
                    $rawChangeData = $app->request()->post('data');
                    if ($rawChangeData) {
                        $changeData = json_decode($rawChangeData, true);
                        $from = new \Tripod\Mongo\MongoGraph();
                        $to = new \Tripod\Mongo\MongoGraph();
                        if (isset($changeData['originalCBDs'])) {
                            foreach ($changeData['originalCBDs'] as $change) {
                                if (is_array($change) && isset($change[_ID_KEY])) {
                                    $from->add_tripod_array($change);
                                }
                            }
                        }
                        if (isset($changeData['newCBDs'])) {
                            foreach ($changeData['newCBDs'] as $change) {
                                if (is_array($change) && isset($change[_ID_KEY])) {
                                    $to->add_tripod_array($change);
                                }
                            }
                        }
                        try {
                            $tripod->saveChanges($from, $to);
                            $app->response()->setStatus(202);
                        } catch (Exception $e) {
                            error_log("POST failed: " . $e->getMessage());
                            $app->response()->setStatus(400);
                        }
                    }
                });
            });
        });
    });
});

/**
 * @param string $format
 * @return string
 */
function getContentType($format)
{
    switch ($format) {
        case FORMAT_RDF_XML:
            $contentType = 'application/rdf+xml';
            break;
        case FORMAT_NTRIPLES:
            $contentType = 'text/plain';
            break;
        case FORMAT_TURTLE:
            $contentType = 'text/turtle';
            break;
        default:
            $contentType = 'application/json';
    }

    return $contentType;
}

/**
 * @param \Tripod\ExtendedGraph $graph
 * @param string $format
 * @return string
 */
function getFormattedGraph(\Tripod\ExtendedGraph $graph, $format)
{
    switch ($format) {
        case FORMAT_RDF_XML:
            $output = $graph->to_rdfxml();
            break;
        case FORMAT_NTRIPLES:
            $output = $graph->to_ntriples();
            break;
        case FORMAT_TURTLE:
            $output = $graph->to_turtle();
            break;
        default:
            $output = $graph->to_json();
    }
    return $output;
}

/**
 * @param \Slim\Slim $app
 * @param array $tripodOptions
 * @return null|Stat
 */
function getStat(Slim\Slim $app, array $tripodOptions)
{
    $statConfig = isset($tripodOptions['statsConfig']) ? $tripodOptions['statsConfig'] : array();
    $statEnv = $app->request()->headers('X-TRIPOD-TEST-ENV');
    if ($statEnv && isset($statConfig['config'])) {
        $statConfig['config']['prefix'] = $statEnv;
    }

    return $statConfig;
}
$app->run();
