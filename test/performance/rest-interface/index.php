<?php
require 'vendor/autoload.php';
define('ARC_DIR', dirname(__FILE__) . '/vendor/semsol/arc2/');
require_once dirname(__FILE__) . '/vendor/talis/tripod-php/src/tripod.inc.php';
define('FORMAT_RDF_XML', 'rdfxml');
define('FORMAT_NTRIPLES', 'ntriples');
define('FORMAT_TURTLE', 'turtle');
define('FORMAT_RDF_JSON', 'rdfjson');
$app = new \Slim\Slim(array(
    'log.level' => \Slim\Log::DEBUG
));

$appConfig = json_decode(file_get_contents('./config/config.json'), true);
require_once dirname(__FILE__) . '/src/Stat.class.php';
if(isset($appConfig['stat']))
{
    StatConfig::setConfig($appConfig['stat']);
}
$tripodOptions = array();
if(isset($appConfig['tripod']))
{
    $tripodOptions = $appConfig['tripod'];
}

$app->group('/1', function() use ($app) {
    $app->group('/:storeName/:podName', function() use ($app)
    {
        $app->group('/graph', function() use ($app) {
            $app->get('/:encodedFqUri', function($storeName, $podName, $encodedFqUri) use ($app) {
                MongoTripodConfig::setConfig(json_decode(file_get_contents('./config/tripod-config-'.$storeName .'.json'), true));
                $tripodOptions['stat'] = getStat($app);
                $tripod = new MongoTripod($podName, $storeName, $tripodOptions);
                $contentType = $app->request()->getMediaType();
                switch($contentType)
                {
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
                if($graph->is_empty())
                {
                    $app->response()->setStatus(404);
                }
                else
                {
                    $output = getFormattedGraph($graph, $format);
                    $app->response()->headers()->set('Content-type', getContentType($format));
                    echo $output;
                }

            });

            $app->delete('/:encodedFqUri', function($storeName, $podName, $encodedFqUri) use ($app) {
                $tripodOptions['stat'] = getStat($app);
                $tripod = new MongoTripod($podName, $storeName, $tripodOptions);
                $oldGraph = $tripod->describeResource(base64_decode($encodedFqUri));
                $tripod->saveChanges($oldGraph, new ExtendedGraph());
            });

            $app->post('/', function($storeName, $podName) use ($app) {
                $tripodOptions['stat'] = getStat($app);
                $tripod = new MongoTripod($podName, $storeName, $tripodOptions);
                $rawGraphData = $app->request()->getBody();
                $graph = new MongoGraph();
                $graph->add_rdf($rawGraphData);
                $tripod->saveChanges(new ExtendedGraph(), $graph);
            });
        });

        $app->group('/change', function() use ($app) {
            $app->post('/', function($storeName, $podName) use ($app) {
              MongoTripodConfig::setConfig(json_decode(file_get_contents('./config/tripod-config-'.$storeName .'.json'), true));
                $app->response()->setStatus(500);
                $tripodOptions['stat'] = getStat($app);
                $tripod = new MongoTripod($podName, $storeName, $tripodOptions);
                $rawChangeData = $app->request()->post('data');
                if($rawChangeData)
                {
                    $changeData = json_decode($rawChangeData, true);
                    $from = new MongoGraph();
                    $to = new MongoGraph();
                    if(isset($changeData['originalCBDs']))
                    {
                        foreach($changeData['originalCBDs'] as $change)
                        {
                            $from->add_tripod_array($change);
                        }
                    }
                    if(isset($changeData['newCBDs']))
                    {
                        foreach($changeData['newCBDs'] as $change)
                        {
                            $to->add_tripod_array($change);
                        }
                    }
                    try
                    {
                        $tripod->saveChanges($from, $to);
                        $app->response()->setStatus(202);
                    }
                    catch (Exception $e)
                    {
                        $app->response()->setStatus(400);
                    }
                }
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
    switch($format)
    {
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
 * @param ExtendedGraph $graph
 * @param string $format
 * @return string
 */
function getFormattedGraph(ExtendedGraph $graph, $format)
{
    switch($format)
    {
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
 * @return null|Stat
 */
function getStat(Slim\Slim $app)
{
    $statEnv = $app->request()->headers('X-TRIPOD-TEST-ENV');
    $storeName = $app->router()->getCurrentRoute()->getParam('storeName');
    $stat = null;
    if(StatConfig::getInstance($app))
    {
        $stat = new Stat($storeName);
        if($statEnv)
        {
            $stat->setStatEnvName($statEnv);
        }
    }
    return $stat;
}
$app->run();
