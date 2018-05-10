<?php

namespace Tripod\Mongo;

/**
 * Class Labeller
 * @package Tripod\Mongo
 */
class Labeller extends \Tripod\Labeller
{
    /**
     * Constructor
     */
    function __construct()
    {
        // only default minimal ns - make app define the rest
        $this->_ns = array(
            'rdf' => 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs' => 'http://www.w3.org/2000/01/rdf-schema#',
            'owl' => 'http://www.w3.org/2002/07/owl#',
            'cs' => 'http://purl.org/vocab/changeset/schema#',
        );
        $config = \Tripod\Config::getInstance();
        $ns = $config->getNamespaces();
        foreach ($ns as $prefix=>$uri)
        {
            $this->set_namespace_mapping($prefix,$uri);
        }
    }

    /**
     * If labeller can generate a qname for this uri, it will return it. Otherwise just returns the original uri
     * @param $uri
     * @return string
     */
    function uri_to_alias($uri) {
        try
        {
            $retVal = $this->uri_to_qname($uri);
        }
        catch (\Tripod\Exceptions\LabellerException $e) {}
        return (empty($retVal)) ? $uri : $retVal;
    }

    /**
     * If labeller can generate a uri for this qname, it will return it. Otherwise just returns the original qname
     * @param $qname
     * @return string
     */
    function qname_to_alias($qname) {
        try
        {
            $retVal = $this->qname_to_uri($qname);
        }
        catch (\Tripod\Exceptions\LabellerException $e) {}
        return (empty($retVal)) ? $qname : $retVal;
    }

    /**
     * @param string $qName
     * @return string
     * @throws \Tripod\Exceptions\LabellerException
     */
    public function qname_to_uri($qName)
    {
        $retVal = parent::qname_to_uri($qName);
        if (empty($retVal)) throw new \Tripod\Exceptions\LabellerException($qName);
        return $retVal;
    }


    // overrides the default behaviour of trying to return a ns even if the prefix is not registered - instead, throw exception
    /**
     * @param string $ns
     * @return string
     * @throws \Tripod\Exceptions\LabellerException
     */
    public function get_prefix($ns) {
        $prefix = array_search($ns, $this->_ns);
        if ( $prefix != null && $prefix !== FALSE) {
            return $prefix;
        }
        else
        {
            throw new \Tripod\Exceptions\LabellerException($ns);
        }
    }
}
