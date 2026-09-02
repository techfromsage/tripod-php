<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use Tripod\Config;
use Tripod\Exceptions\LabellerException;

class Labeller extends \Tripod\Labeller
{
    /**
     * Constructor.
     */
    public function __construct()
    {
        // only default minimal ns - make app define the rest
        $this->_ns = [
            'rdf' => 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs' => 'http://www.w3.org/2000/01/rdf-schema#',
            'owl' => 'http://www.w3.org/2002/07/owl#',
            'cs' => 'http://purl.org/vocab/changeset/schema#',
        ];
        $config = Config::getInstance();
        $ns = $config->getNamespaces();
        foreach ($ns as $prefix => $uri) {
            $this->set_namespace_mapping($prefix, $uri);
        }
    }

    /**
     * If labeller can generate a qname for this uri, it will return it. Otherwise just returns the original uri.
     *
     * @return ($uri is null ? null : string)
     */
    public function uri_to_alias(?string $uri): ?string
    {
        try {
            $retVal = $this->uri_to_qname($uri);
        } catch (LabellerException $e) {
        }

        return (empty($retVal)) ? $uri : $retVal;
    }

    /**
     * If labeller can generate a uri for this qname, it will return it. Otherwise just returns the original qname.
     *
     * @return ($qName is null ? null : string)
     */
    public function qname_to_alias(?string $qName): ?string
    {
        try {
            $retVal = $this->qname_to_uri($qName);
        } catch (LabellerException $e) {
        }

        return (empty($retVal)) ? $qName : $retVal;
    }

    /**
     * Unlike the base labeller this never returns null: unresolvable qnames (and null input) throw instead.
     *
     * @return ($qName is null ? null : string)
     *
     * @throws LabellerException
     */
    public function qname_to_uri(?string $qName): ?string
    {
        $retVal = parent::qname_to_uri($qName);
        if (empty($retVal)) {
            throw new LabellerException($qName);
        }

        return $retVal;
    }

    // overrides the default behaviour of trying to return a ns even if the prefix is not registered - instead, throw exception
    /**
     * @throws LabellerException
     */
    public function get_prefix(string $ns): string
    {
        $prefix = array_search($ns, $this->_ns, true);
        if ($prefix !== false) {
            return $prefix;
        }

        throw new LabellerException($ns);
    }
}
