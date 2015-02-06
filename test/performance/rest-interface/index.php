<?php
require 'vendor/autoload.php';
define('ARC_DIR', dirname(__FILE__) . '/vendor/semsol/arc2/');
require_once 'vendor/talis/tripod-php/src/tripod.inc.php';

$app = new \Slim\Slim();

MongoTripodConfig::setConfig(json_decode(file_get_contents('config/tripod-config.json'), true));


$app->group('/1', function() use ($app) {
    $app->group('/:storeName/:podName', function() use ($app)
    {
        $app->group('/graph', function() use ($app) {
            $app->get('/:encodedFqUri', function($storeName, $podName, $encodedFqUri) use ($app) {
                $tripod = new MongoTripod($podName, $storeName);
                $graph =  $tripod->describeResource(base64_decode($encodedFqUri));
                $contentType = $app->request()->getMediaType();
                switch($contentType)
                {
                    case 'application/rdf+xml':
                        $output = $graph->to_rdfxml();
                        break;
                    case 'text/plain':
                        $output = $graph->to_ntriples();
                        break;
                    case 'text/turtle':
                        $output = $graph->to_turtle();
                        break;
                    default:
                        $contentType = 'application/json';
                        $output = $graph->to_json();
                }

                $app->response()->headers()->set('Content-type', $contentType);
                echo $output;

            });

            $app->delete('/:encodedFqUri', function($storeName, $podName, $encodedFqUri) use ($app) {
                $tripod = new MongoTripod($podName, $storeName);
                $oldGraph = $tripod->describeResource(base64_decode($encodedFqUri));
                $tripod->saveChanges($oldGraph, new ExtendedGraph());
            });

            $app->post('/', function($storeName, $podName) use ($app) {
                $tripod = new MongoTripod($podName, $storeName);
                $rawGraphData = $app->request()->getBody();
                $graph = new MongoGraph();
                $graph->add_rdf($rawGraphData);
                $tripod->saveChanges(new ExtendedGraph(), $graph);
            });
        });

        $app->group('/change', function() use ($app) {
            $app->post('/', function($storeName, $podName) use ($app) {
                $tripod = new MongoTripod($podName, $storeName);
                $rawChangeData = $app->request()->post('data');
                if($rawChangeData)
                {
                    $changeData = json_decode($rawChangeData, true);
                    $from = new MongoGraph();
                    $to = new MongoGraph();
                    if(isset($changeData['oldCBDs']))
                    {
                        $from->add_tripod_array($changeData['oldCBDs']);
                    }
                    if(isset($changeData['newCBDs']))
                    {
                        $to->add_tripod_array($changeData['newCBDs']);
                    }
                    $tripod->saveChanges($from, $to);
                }
            });
        });
    });
});

$app->run();