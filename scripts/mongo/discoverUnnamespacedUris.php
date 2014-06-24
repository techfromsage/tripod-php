<?php
/**
 * Detects un-namespaced subjects or object uris in CBD collections of the target database. Optionally supply a base uri to match against that rather than all uris
 */
if ($argc!=4 && $argc!=3)
{
    echo "usage: php discoverUnnamespacedUris.php connStr database [baseUri]";
    die();
}

array_shift($argv);
$client = new MongoClient($argv[0]);

/* @var $db MongoDB */
$db = $client->selectDb($argv[1]);

function isUnNamespaced($uri,$baseUri=null)
{
    if ($baseUri==null)
    {
        return (strpos($uri,'http://')===0 || strpos($uri,'https://')===0);
    }
    else
    {
        return strpos($uri,$baseUri)===0;
    }
}

$results = array();
foreach ($db->listCollections() as $collection)
{
    /* @var $collection MongoCollection */
    if (strpos($collection->getName(),'CBD_')===0) // only process CBD_collections
    {
        echo "Checking out {$collection->getName()}\n";
        $count = 0;
        foreach ($collection->find() as $doc)
        {
            if (!isset($doc['_id']) || !isset($doc['_id']['r']))
            {
                echo "  Illegal doc: no _id or missing _id.r";
            }
            else
            {
                if (isUnNamespaced($doc['_id']['r'],$argv[2]))
                {
                    echo "  Un-namespaced subject: {$doc['_id']['r']}\n";
                    $count++;
                }
            }
            foreach ($doc as $property=>$value)
            {
                if (strpos($property,"_")===0) // ignore meta fields, _id, _version, _uts etc.
                {
                    continue;
                }
                else
                {
                    if (isset($value['l']))
                    {
                        // ignore, is a literal
                        continue;
                    }
                    else if (isset($value['u']))
                    {
                        if (isUnNamespaced($value['u'],$argv[2]))
                        {
                            echo "  Un-namespaced object uri (single value): {$value['u']}\n";
                            $count++;
                        }
                    }
                    else
                    {
                        foreach ($value as $v)
                        {
                            if (isset($v['u']))
                            {
                                if (isUnNamespaced($v['u'],$argv[2]))
                                {
                                    echo "  Un-namespaced object uri (multiple value): {$v['u']}\n";
                                    $count++;
                                }
                            }
                        }
                    }
                }
            }
        }
        $results[] = "{$collection->getName()} has $count un-namespaced uris";
        echo "Done with {$collection->getName()}\n";
    }
}
echo "\n".implode("\n",$results)."\n";

?>