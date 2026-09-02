<?php

declare(strict_types=1);

namespace Tripod;

/**
 * Utility class for labelling properties
 * Based on Moriarty's Labeller.
 *
 * @see https://code.google.com/p/moriarty/source/browse/trunk/labeller.class.php
 */
class Labeller
{
    /**
     * @var array<string, non-empty-list<string>>
     */
    public array $_labels = [
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_1' => ['first', 'first', 'is first member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_2' => ['second', 'second', 'is second member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_3' => ['third', 'third', 'is third member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_4' => ['fourth', 'fourth', 'is fourth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_5' => ['fifth', 'fifth', 'is fifth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_6' => ['sixth', 'sixth', 'is sixth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_7' => ['seventh', 'seventh', 'is seventh member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_8' => ['eighth', 'eighth', 'is eighth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_9' => ['ninth', 'ninth', 'is ninth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_10' => ['tenth', 'tenth', 'is tenth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_11' => ['eleventh', 'eleventh', 'is eleventh member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_12' => ['twelfth', 'twelfth', 'is twelfth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_13' => ['thirteenth', 'thirteenth', 'is thirteenth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_14' => ['fourteenth', 'fourteenth', 'is fourteenth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_15' => ['fifteenth', 'fifteenth', 'is fifteenth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_16' => ['sixteenth', 'sixteenth', 'is sixteenth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_17' => ['seventeenth', 'seventeenth', 'is seventeenth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_18' => ['eighteenth', 'eighteenth', 'is eighteenth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_19' => ['nineteenth', 'nineteenth', 'is nineteenth member of'],
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_20' => ['twentieth', 'twentieth', 'is twentieth member of'],

        'http://www.w3.org/2000/01/rdf-schema#seeAlso' => ['see also', 'see also', 'is see also of'],
        'http://www.w3.org/2000/01/rdf-schema#isDefinedBy' => ['defined by', 'defined by', 'defines'],
        'http://www.w3.org/2000/01/rdf-schema#subClassOf' => ['subclass of', 'subclass of', 'is superclass of'],

        'http://www.w3.org/2002/07/owl#imports' => ['imports', 'imports', 'is imported by'],
        'http://www.w3.org/2002/07/owl#sameAs' => ['same as', 'same as', 'same as'],

        'http://xmlns.com/foaf/0.1/isPrimaryTopicOf' => ['is the primary topic of', 'is the primary topic of', 'primary topic'],
        'http://xmlns.com/foaf/0.1/primaryTopic' => ['primary topic', 'primary topics', 'is the primary topic of'],
        'http://xmlns.com/foaf/0.1/weblog' => ['blog', 'blogs', 'is weblog of'],
        'http://xmlns.com/foaf/0.1/knows' => ['knows', 'knows', 'knows'],
        'http://xmlns.com/foaf/0.1/firstName' => ['first name', 'first names', 'is first name of'],
        'http://xmlns.com/foaf/0.1/depiction' => ['picture', 'pictures', 'is picture of'],
        'http://xmlns.com/foaf/0.1/nick' => ['nickname', 'nickname', 'is nickname of'],
        'http://xmlns.com/foaf/0.1/phone' => ['phone number'],
        'http://xmlns.com/foaf/0.1/mbox' => ['email address'],
        'http://xmlns.com/foaf/0.1/workplaceHomepage' => ["workplace's homepage"],
        'http://xmlns.com/foaf/0.1/schoolHomepage' => ["school's homepage"],
        'http://xmlns.com/foaf/0.1/openid' => ['OpenID'],
        'http://xmlns.com/foaf/0.1/mbox_sha1sum' => ['email address hashcode'],
        'http://xmlns.com/foaf/0.1/made' => ['made', 'made', 'maker'],
        'http://xmlns.com/foaf/0.1/accountProfilePage' => ['account profile page'],
        'http://xmlns.com/foaf/0.1/accountName' => ['account name'],
        'http://xmlns.com/foaf/0.1/accountServiceHomepage' => ['account service homepage'],
        'http://xmlns.com/foaf/0.1/holdsAccount' => ['account', 'accounts', 'is account held by'],

        'http://rdfs.org/sioc/ns#content' => ['content'],
        'http://rdfs.org/sioc/ns#hasContainer' => ['container'],

        'http://purl.org/dc/elements/1.1/identifier' => ['general identifier', 'general identifiers', 'is general identifier of'],
        'http://purl.org/dc/elements/1.1/type' => ['document type', 'document types', 'is document type of'],
        'http://purl.org/dc/elements/1.1/rights' => ['rights statement', 'right statements', 'is rights statement for'],

        'http://purl.org/dc/terms/abstract' => ['abstract', 'abstracts', 'is abstract of'],
        'http://purl.org/dc/terms/accessRights' => ['access rights', 'access rights', 'are access rights for'],
        'http://purl.org/dc/terms/alternative' => ['alternative title', 'alternative titles', 'is alternative title for'],
        'http://purl.org/dc/terms/audience' => ['audience', 'audiences', 'is audience for'],
        'http://purl.org/dc/terms/available' => ['date available', 'dates available', 'is date available of'],
        'http://purl.org/dc/terms/bibliographicCitation' => ['bibliographic citation', 'bibliographic citations', 'is bibliographic citation of'],
        'http://purl.org/dc/terms/contributor' => ['contributor', 'contributors', 'is contributor to'],
        'http://purl.org/dc/terms/coverage' => ['coverage', 'coverage', 'is coverage of'],
        'http://purl.org/dc/terms/created' => ['date created', 'dates created', 'is date created of'],
        'http://purl.org/dc/terms/creator' => ['creator', 'creators', 'is creator of'],
        'http://purl.org/dc/terms/date' => ['date', 'dates', 'is date of'],
        'http://purl.org/dc/terms/dateAccepted' => ['date accepted', 'dates accepted', 'is date accepted of'],
        'http://purl.org/dc/terms/dateCopyrighted' => ['date copyrighted', 'dates copyrighted', 'is date copyrighted of'],
        'http://purl.org/dc/terms/dateSubmitted' => ['date submitted', 'dates submitted', 'is date submitted of'],
        'http://purl.org/dc/terms/description' => ['description', 'descriptions', 'is description of'],
        'http://purl.org/dc/terms/format' => ['format', 'formats', 'is format of'],
        'http://purl.org/dc/terms/hasPart' => ['has part', 'has parts', 'is part of'],
        'http://purl.org/dc/terms/hasVersion' => ['version', 'versions', 'version of'],
        'http://purl.org/dc/terms/identifier' => ['identifier', 'identifiers', 'is identifier of'],
        'http://purl.org/dc/terms/isPartOf' => ['part of', 'part of', 'part'],
        'http://purl.org/dc/terms/isReferencedBy' => ['is referenced by', 'is referenced by', 'references'],
        'http://purl.org/dc/terms/isReplacedBy' => ['is replaced by', 'is replaced by', 'replaces'],
        'http://purl.org/dc/terms/isRequiredBy' => ['is required by', 'is required by', 'requires'],
        'http://purl.org/dc/terms/issued' => ['date issued', 'dates issued', 'is date issued of'],
        'http://purl.org/dc/terms/isVersionOf' => ['version of', 'version of', 'version'],
        'http://purl.org/dc/terms/language' => ['language', 'languages', 'is language of'],
        'http://purl.org/dc/terms/medium' => ['medium', 'media', 'is medium of'],
        'http://purl.org/dc/terms/modified' => ['date modified', 'dates modified', 'is date modified of'],
        'http://purl.org/dc/terms/references' => ['references', 'references', 'is referenced by'],
        'http://purl.org/dc/terms/replaces' => ['replaces', 'replaces', 'is replaced by'],
        'http://purl.org/dc/terms/requires' => ['requires', 'requires', 'is required by'],
        'http://purl.org/dc/terms/rights' => ['rights statement', 'right statements', 'is rights statement for'],
        'http://purl.org/dc/terms/rightsHolder' => ['rights holder', 'rights holders', 'is rights holder of'],
        'http://purl.org/dc/terms/tableOfContents' => ['table of contents', 'tables of contents', 'is table of contents of'],
        'http://purl.org/dc/terms/type' => ['document type', 'document types', 'is document type of'],
        'http://purl.org/dc/terms/updated' => ['date updated', 'dates updated', 'is date updated of'],
        'http://purl.org/dc/terms/valid' => ['date valid', 'dates valid', 'is date valid of'],

        'http://www.w3.org/2003/01/geo/wgs84_pos#lat' => ['latitude', 'latitudes', 'is latitude of'],
        'http://www.w3.org/2003/01/geo/wgs84_pos#long' => ['longitude', 'longitudes', 'is longitude of'],

        'http://purl.org/vocab/bio/0.1/olb' => ['one line bio', 'one line bios', 'is one line bio of'],
        'http://purl.org/vocab/bio/0.1/event' => ['life event', 'life events', 'is life event of'],

        'http://purl.org/vocab/relationship/parentOf' => ['is parent of', 'is parent of', 'is child of'],
        'http://purl.org/vocab/relationship/childOf' => ['is child of', 'is child of', 'is parent of'],
        'http://purl.org/vocab/relationship/spouseOf' => ['spouse', 'spouses', 'spouse'],
        'http://purl.org/vocab/relationship/acquaintanceOf' => ['acquaintance'],
        'http://purl.org/vocab/relationship/friendOf' => ['friend'],

        'http://purl.org/vocab/vann/example' => ['example', 'examples', 'is example for'],
        'http://purl.org/vocab/vann/preferredNamespacePrefix' => ['preferred namespace prefix', 'preferred namespace prefixes', 'is preferred namespace prefix for'],
        'http://purl.org/vocab/vann/preferredNamespaceUri' => ['preferred namespace URI', 'preferred namespace URIs', 'is preferred namespace URI for'],
        'http://purl.org/vocab/vann/changes' => ['change log', 'change logs', 'is change log of'],

        'http://www.w3.org/2004/02/skos/core#prefLabel' => ['preferred label', 'preferred labels', 'is preferred label of'],
        'http://www.w3.org/2004/02/skos/core#altLabel' => ['alternative label', 'alternative labels', 'is alternative label of'],
        'http://www.w3.org/2004/02/skos/core#hiddenLabel' => ['hidden label', 'hidden labels', 'is hidden label of'],
        'http://www.w3.org/2004/02/skos/core#related' => ['related concept', 'related concepts', 'is related concept of'],
        'http://www.w3.org/2004/02/skos/core#broader' => ['broader concept', 'broader concepts', 'narrower concept'],
        'http://www.w3.org/2004/02/skos/core#narrower' => ['narrower concept', 'narrower concepts', 'broader concept'],
        'http://www.w3.org/2004/02/skos/core#scopeNote' => ['scope note', 'scope notes', 'is scope note of'],
        'http://www.w3.org/2004/02/skos/core#example' => ['example', 'examples', 'is example of'],
        'http://www.w3.org/2004/02/skos/core#historyNote' => ['history note', 'history notes', 'is history note of'],
        'http://www.w3.org/2004/02/skos/core#editorialNote' => ['editorial note', 'editorial notes', 'is editorial note of'],
        'http://www.w3.org/2004/02/skos/core#changeNote' => ['change note', 'change notes', 'is change note of'],
        'http://www.w3.org/2004/02/skos/core#inScheme' => ['scheme', 'schemes', 'is scheme of'],
        'http://www.w3.org/2004/02/skos/core#hasTopConcept' => ['top concept', 'top concepts', 'is top concept of'],
        'http://www.w3.org/2004/02/skos/core#exactMatch' => ['exact match', 'exact matches', 'is exact match of'],
        'http://www.w3.org/2004/02/skos/core#closeMatch' => ['close match', 'close matches', 'is close match of'],
        'http://www.w3.org/2004/02/skos/core#broadMatch' => ['broad match', 'broad matches', 'is broad match of'],
        'http://www.w3.org/2004/02/skos/core#narrowMatch' => ['narrow match', 'narrow matches', 'is narrow match of'],
        'http://www.w3.org/2004/02/skos/core#relatedMatch' => ['related match', 'related matches', 'is related match of'],

        'http://rdfs.org/ns/void#exampleResource' => ['example resource', 'example resources', 'is example resource of'],
        'http://rdfs.org/ns/void#sparqlEndpoint' => ['SPARQL endpoint', 'SPARQL endpoints', 'is SPARQL endpoint of'],
        'http://rdfs.org/ns/void#uriLookupEndpoint' => ['URI lookup point', 'URI lookup points', 'is URI lookup point of'],
        'http://rdfs.org/ns/void#dataDump' => ['data dump', 'data dumps', 'is data dump of'],
        'http://rdfs.org/ns/void#vocabulary' => ['vocabulary used', 'vocabularies used', 'is vocabulary used in'],
        'http://rdfs.org/ns/void#uriRegexPattern' => ['URI regex pattern'],

        'http://open.vocab.org/terms/numberOfPages' => ['number of pages', 'numbers of pages', 'is number of pages of'],
        'http://open.vocab.org/terms/subtitle' => ['sub-title', 'sub-titles', 'is sub-title of'],
        'http://open.vocab.org/terms/firstSentence' => ['first sentence'],
        'http://open.vocab.org/terms/category' => ['category', 'categories'],

        'http://purl.org/ontology/bibo/pageStart' => ['first page'],
        'http://purl.org/ontology/bibo/pageEnd' => ['last page'],
        'http://purl.org/ontology/bibo/issn' => ['ISSN', 'ISSNs', 'is ISSN of'],
        'http://purl.org/ontology/bibo/eissn' => ['EISSN', 'EISSNs', 'is EISSN of'],
        'http://purl.org/ontology/bibo/isbn' => ['ISBN', 'ISBNs', 'is ISBN of'],
        'http://purl.org/ontology/bibo/isbn10' => ['10 digit ISBN', '10 digit ISBNs', 'is 10 digit ISBN of'],
        'http://purl.org/ontology/bibo/isbn13' => ['13 digit ISBN', '13 digit ISBNs', 'is 13 digit ISBN of'],
        'http://purl.org/ontology/bibo/lccn' => ['LCCN', 'LCCNs', 'is LCCN of'],
        'http://purl.org/ontology/bibo/doi' => ['DOI', 'DOIs', 'is DOI of'],
        'http://purl.org/ontology/bibo/oclcnum' => ['OCLC number', 'OCLC numbers', 'is OCLC number of'],
        'http://purl.org/ontology/bibo/contributorList' => ['list of contributors', 'lists of contributors', 'is list of contributors to'],
        'http://purl.org/ontology/bibo/authorList' => ['list of authors', 'lists of authors', 'is list of authors of'],

        'http://purl.org/ontology/mo/wikipedia' => ['wikipedia page', 'wikipedia pages', 'is wikipedia page of'],
        'http://purl.org/ontology/mo/discogs' => ['discogs page', 'discogs pages', 'is discogs page of'],

        'http://purl.org/ontology/po/series' => ['series', 'series'],
        'http://purl.org/ontology/po/medium_synopsis' => ['medium synopsis', 'medium synopses'],
        'http://purl.org/ontology/po/short_synopsis' => ['short synopsis', 'short synopses'],
        'http://purl.org/ontology/po/long_synopsis' => ['long synopsis', 'long synopses'],
        'http://purl.org/ontology/po/format' => ['programme format'],
        'http://purl.org/ontology/po/masterbrand' => ['master  brand'],

        'http://purl.org/net/schemas/space/actor' => ['actor', 'actors', 'performed'],
        'http://purl.org/net/schemas/space/performed' => ['performed', 'performed', 'actor'],
        'http://purl.org/net/schemas/space/missionRole' => ['mission role'],
        'http://purl.org/net/schemas/space/alternateName' => ['alternate name'],
        'http://purl.org/net/schemas/space/mass' => ['mass'],
        'http://purl.org/net/schemas/space/spacecraft' => ['spacecraft', 'spacecraft'],
        'http://purl.org/net/schemas/space/agency' => ['agency'],
        'http://purl.org/net/schemas/space/launch' => ['launch', 'launches'],
        'http://purl.org/net/schemas/space/launchvehicle' => ['launch vehicle'],
        'http://purl.org/net/schemas/space/launchsite' => ['launch site'],
        'http://purl.org/net/schemas/space/launched' => ['launched', 'launched'],
        'http://purl.org/net/schemas/space/country' => ['country', 'countries'],

        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#borders' => ['borders', 'borders', 'borders'],
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasCensusCode' => ['census code'],
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasOfficialName' => ['official name'],
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasOfficialWelshName' => ['official welsh name'],
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasVernacularName' => ['vernacular name'],
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasBoundaryLineName' => ['boundary line name'],
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#completelySpatiallyContains' => ['completely spatially contains', 'completely spatially contains', 'is completely spatially contained by'],
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#tangentiallySpatiallyContains' => ['tangentially spatially contains', 'tangentially spatially contains', 'is tangentially spatially contained by'],
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#isSpatiallyEqualTo' => ['spatially equal to', 'spatially equal to', 'spatially equal to'],

        'http://rdvocab.info/Elements/placeOfPublication' => ['place of publication', 'places of publication'],

        'http://www.w3.org/2000/10/swap/pim/contact#nearestAirport' => ['nearest airport'],

        'http://www.daml.org/2001/10/html/airport-ont#icao' => ['ICAO', 'ICAOs', 'is ICAO of'],
        'http://www.daml.org/2001/10/html/airport-ont#iata' => ['IATA', 'IATAs', 'is IATA of'],

        'http://schemas.talis.com/2005/address/schema#regionName' => ['region name'],
        'http://schemas.talis.com/2005/address/schema#streetAddress' => ['street address'],
        'http://schemas.talis.com/2005/address/schema#localityName' => ['locality name'],
        'http://schemas.talis.com/2005/address/schema#postalCode' => ['postal code'],

        'http://schemas.talis.com/2006/recordstore/schema#tags' => ['tag'],
        'http://schemas.talis.com/2006/recordstore/schema#changeReason' => ['reason for change', 'reasons for change'],
        'http://schemas.talis.com/2006/recordstore/schema#active' => ['is active?', 'is active?'],
        'http://schemas.talis.com/2006/recordstore/schema#createdDate' => ['date created', 'dates created'],
        'http://schemas.talis.com/2006/recordstore/schema#previousState' => ['previous state'],
        'http://schemas.talis.com/2006/recordstore/schema#appliedBy' => ['applied by', 'applied by'],
        'http://schemas.talis.com/2006/recordstore/schema#appliedDate' => ['date applied', 'dates applied'],
        'http://schemas.talis.com/2006/recordstore/schema#reason' => ['reason'],

        'http://schemas.talis.com/2005/dir/schema#etag' => ['ETag'],

        'http://www.gazettes-online.co.uk/ontology#isAbout' => ['about', 'about'],
        'http://www.gazettes-online.co.uk/ontology#isInIssue' => ['issue'],
        'http://www.gazettes-online.co.uk/ontology/location#hasAddress' => ['address', 'addresses'],
        'http://www.gazettes-online.co.uk/ontology/court#courtName' => ['court name'],
        'http://www.gazettes-online.co.uk/ontology/court#sitsAt' => ['sits at', 'sits at'],

        'http://www.geonames.org/ontology#parentFeature' => ['parent feature'],

        'http://purl.org/goodrelations/v1#hasEAN_UCC-13' => ['EAN-13'],
        'http://purl.org/goodrelations/v1#hasMPN' => ['Manufacturer Part Number'],
        'http://purl.org/goodrelations/v1#hasGTIN-14' => ['GTIN-14'],
    ];

    /**
     * @var string[]
     */
    protected array $_label_properties = [];

    /**
     * @var array<string, string>
     */
    protected array $_ns = [
        'rdf' => 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs' => 'http://www.w3.org/2000/01/rdf-schema#',
        'owl' => 'http://www.w3.org/2002/07/owl#',
        'cs' => 'http://purl.org/vocab/changeset/schema#',
        'bf' => 'http://schemas.talis.com/2006/bigfoot/configuration#',
        'frm' => 'http://schemas.talis.com/2006/frame/schema#',

        'dc' => 'http://purl.org/dc/elements/1.1/',
        'dct' => 'http://purl.org/dc/terms/',
        'dctype' => 'http://purl.org/dc/dcmitype/',

        'foaf' => 'http://xmlns.com/foaf/0.1/',
        'bio' => 'http://purl.org/vocab/bio/0.1/',
        'geo' => 'http://www.w3.org/2003/01/geo/wgs84_pos#',
        'rel' => 'http://purl.org/vocab/relationship/',
        'rss' => 'http://purl.org/rss/1.0/',
        'wn' => 'http://xmlns.com/wordnet/1.6/',
        'air' => 'http://www.daml.org/2001/10/html/airport-ont#',
        'contact' => 'http://www.w3.org/2000/10/swap/pim/contact#',
        'ical' => 'http://www.w3.org/2002/12/cal/ical#',
        'icaltzd' => 'http://www.w3.org/2002/12/cal/icaltzd#',
        'frbr' => 'http://purl.org/vocab/frbr/core#',

        'ad' => 'http://schemas.talis.com/2005/address/schema#',
        'lib' => 'http://schemas.talis.com/2005/library/schema#',
        'dir' => 'http://schemas.talis.com/2005/dir/schema#',
        'user' => 'http://schemas.talis.com/2005/user/schema#',
        'sv' => 'http://schemas.talis.com/2005/service/schema#',
        'mo' => 'http://purl.org/ontology/mo/',
        'status' => 'http://www.w3.org/2003/06/sw-vocab-status/ns#',
        'label' => 'http://purl.org/net/vocab/2004/03/label#',
        'skos' => 'http://www.w3.org/2004/02/skos/core#',
        'bibo' => 'http://purl.org/ontology/bibo/',
        'ov' => 'http://open.vocab.org/terms/',
        'void' => 'http://rdfs.org/ns/void#',
        'xsd' => 'http://www.w3.org/2001/XMLSchema#',
        'dbp' => 'http://dbpedia.org/resource/',
        'dbpo' => 'http://dbpedia.org/ontology/',
        'wiki' => 'http://en.wikipedia.org/wiki/',
        'gn' => 'http://www.geonames.org/ontology#',
        'cyc' => 'http://sw.opencyc.org/2009/04/07/concept/en/',
    ];

    /**
     * Map a portion of a URI to a short prefix for use when serialising the graph.
     *
     * @param string $prefix the namespace prefix to associate with the URI
     * @param string $uri    the URI to associate with the prefix
     */
    public function set_namespace_mapping(string $prefix, string $uri): void
    {
        $this->_ns[$prefix] = $uri;
    }

    /**
     * Convert a QName to a URI using registered namespace prefixes.
     *
     * @param string|null $qName the QName to convert
     *
     * @return string|null the URI corresponding to the QName if a suitable prefix exists, null otherwise
     */
    public function qname_to_uri(?string $qName): ?string
    {
        if ($qName === null || !preg_match('~^(.+):(.+)$~', $qName, $m)) {
            return null;
        }

        if (isset($this->_ns[$m[1]])) {
            return $this->_ns[$m[1]] . $m[2];
        }

        return null;
    }

    /**
     * Convert a URI to a QName using registered namespace prefixes.
     *
     * @param string|null $uri the URI to convert
     *
     * @return string|null the QName corresponding to the URI if a suitable prefix exists, null otherwise
     */
    public function uri_to_qname(?string $uri): ?string
    {
        if ($uri !== null && preg_match('~^(.*[\/\#])([a-z0-9\-\_\:]+)$~i', $uri, $m)) {
            $ns = $m[1];
            $localname = $m[2];
            $prefix = $this->get_prefix($ns);
            if ($prefix) {
                return $prefix . ':' . $localname;
            }
        }

        return null;
    }

    public function get_prefix(string $ns): string
    {
        $prefix = array_search($ns, $this->_ns, true);
        if ($prefix) {
            return $prefix;
        }

        /** @var non-empty-list<string> $parts */
        $parts = preg_split('/[\/#]/', $ns);
        for ($i = count($parts) - 1; $i >= 0; $i--) {
            if (
                preg_match('~^[a-zA-Z][a-zA-Z0-9\-]+$~', $parts[$i])
                && !isset($this->_ns[$parts[$i]])
                && $parts[$i] != 'schema'
                && $parts[$i] != 'ontology'
                && $parts[$i] != 'vocab'
                && $parts[$i] != 'terms'
                && $parts[$i] != 'ns'
                && $parts[$i] != 'core'
                && strlen($parts[$i]) > 3
            ) {
                $prefix = strtolower($parts[$i]);
                $this->_ns[$prefix] = $ns;

                return $prefix;
            }
        }

        $index = 0;
        while (isset($this->_ns['ns' . $index])) {
            $index++;
        }

        $prefix = 'msg' . $index;
        $this->_ns[$prefix] = $ns;

        return $prefix;
    }

    public function add_labelling_property(string $p): void
    {
        $this->_label_properties[] = $p;
    }

    public function get_ns(): array
    {
        return $this->_ns;
    }

    public function get_label(string $uri, ?ExtendedGraph $g = null, bool $capitalize = false, bool $use_qnames = false): string
    {
        if ($g instanceof ExtendedGraph) {
            $label = (string) $g->get_first_literal($uri, 'http://www.w3.org/2004/02/skos/core#prefLabel', '', 'en');
            if ($label) {
                return $label;
            }

            $label = (string) $g->get_first_literal($uri, RDFS_LABEL, '', 'en');
            if ($label) {
                return $label;
            }

            $label = (string) $g->get_first_literal($uri, 'http://purl.org/dc/terms/title', '', 'en');
            if ($label) {
                return $label;
            }

            $label = (string) $g->get_first_literal($uri, DC_TITLE, '', 'en');
            if ($label) {
                return $label;
            }

            $label = (string) $g->get_first_literal($uri, FOAF_NAME, '', 'en');
            if ($label) {
                return $label;
            }

            $label = (string) $g->get_first_literal($uri, 'http://www.geonames.org/ontology#name', '', 'en');
            if ($label) {
                return $label;
            }

            $label = (string) $g->get_first_literal($uri, RDF_VALUE, '', 'en');
            if ($label) {
                return $label;
            }

            $label = (string) $g->get_first_literal($uri, 'http://purl.org/rss/1.0/title', '', 'en');
            if ($label) {
                return $label;
            }

            foreach ($this->_label_properties as $p) {
                $label = (string) $g->get_first_literal($uri, $p, '', 'en');
                if ($label) {
                    return $label;
                }
            }
        }

        if (isset($this->_labels[$uri])) {
            if ($capitalize) {
                return ucfirst($this->_labels[$uri][0]);
            }

            return $this->_labels[$uri][0];
        }

        if (preg_match('~^http://www.w3.org/1999/02/22-rdf-syntax-ns#_(.+)$~', $uri, $m)) {
            if ($capitalize) {
                return 'Item ' . $m[1];
            }

            return 'item ' . $m[1];
        }

        if ($use_qnames) {
            $label = $this->uri_to_qname($uri);
            if ($label) {
                return $label;
            }
        } elseif (preg_match('~^.*[\/\#]([^\/\#]+)$~', $uri, $m)) {
            $localname = $m[1];
            if (preg_match('~[^A-Z][A-Z][^A-Z]~', $localname)) {
                /** @var non-empty-list<string> $parts */
                $parts = preg_split('/([A-Z][^A-Z]*)/', $localname, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                $parts = array_map('strtolower', $parts);
                if ($parts[0] == 'has') {
                    array_shift($parts);
                }

                $label = implode(' ', $parts);
                if ($capitalize) {
                    return ucfirst($label);
                }

                return $label;
            }

            if ($capitalize && preg_match('~^[a-z]~', $localname)) {
                return ucfirst($localname);
            }

            return $localname;
        }

        return $uri;
    }

    public function get_plural_label(string $uri, ?ExtendedGraph $g = null, bool $capitalize = false, bool $use_qnames = false): string
    {
        if ($g instanceof ExtendedGraph) {
            $label = (string) $g->get_first_literal($uri, 'http://purl.org/net/vocab/2004/03/label#plural', '', 'en');
            if ($label) {
                return $label;
            }
        }

        if (isset($this->_labels[$uri])) {
            $label = count($this->_labels[$uri]) > 1 ? $this->_labels[$uri][1] : $this->_labels[$uri][0] . 's';

            if ($capitalize) {
                return ucfirst($label);
            }

            return $label;
        }

        if ($use_qnames === false && preg_match('~^.*[\/\#]([a-z]+)$~', $uri, $m)) {
            return $m[1] . 's';
        }

        $label = $this->get_label($uri, $g, $capitalize, $use_qnames);
        if (preg_match('~^[^\s]+$~', $label)) {
            return $label . 's';
        }

        return $label;
    }

    public function get_inverse_label(string $uri, ?ExtendedGraph $g = null, bool $capitalize = false, bool $use_qnames = false): string
    {
        if ($g instanceof ExtendedGraph) {
            $label = (string) $g->get_first_literal($uri, 'http://purl.org/net/vocab/2004/03/label#inverseSingular', '', 'en');
            if ($label) {
                return $label;
            }
        }

        if (isset($this->_labels[$uri])) {
            $label = count($this->_labels[$uri]) > 2 ? $this->_labels[$uri][2] : 'is ' . $this->_labels[$uri][0] . ' of';

            if ($capitalize) {
                return ucfirst($label);
            }

            return $label;
        }

        $label = 'is ' . $this->get_label($uri, $g, false, $use_qnames) . ' of';
        if ($capitalize) {
            return ucfirst($label);
        }

        return $label;
    }

    public function label_graph(ExtendedGraph &$graph): void
    {
        $labelled_properties = [];
        $index = $graph->get_index();
        foreach ($index as $p_list) {
            foreach ($p_list as $p => $val) {
                if (!array_key_exists($p, $labelled_properties)) {
                    if (isset($this->_labels[$p])) {
                        if (!$graph->subject_has_property($p, RDFS_LABEL)) {
                            $graph->add_literal_triple($p, RDFS_LABEL, $this->_labels[$p][0]);
                        }

                        if (!$graph->subject_has_property($p, 'http://purl.org/net/vocab/2004/03/label#plural')) {
                            if (count($this->_labels[$p]) > 1) {
                                $graph->add_literal_triple($p, 'http://purl.org/net/vocab/2004/03/label#plural', $this->_labels[$p][1]);
                            } else {
                                $graph->add_literal_triple($p, 'http://purl.org/net/vocab/2004/03/label#plural', $this->_labels[$p][0] . 's');
                            }
                        }

                        if (!$graph->subject_has_property($p, 'http://purl.org/net/vocab/2004/03/label#inverseSingular')) {
                            if (count($this->_labels[$p]) > 2) {
                                $graph->add_literal_triple($p, 'http://purl.org/net/vocab/2004/03/label#inverseSingular', $this->_labels[$p][2]);
                            } else {
                                $graph->add_literal_triple($p, 'http://purl.org/net/vocab/2004/03/label#inverseSingular', 'is ' . $this->_labels[$p][0] . ' of');
                            }
                        }

                        $labelled_properties[$p] = 1;
                    } elseif (preg_match('~^http://www.w3.org/1999/02/22-rdf-syntax-ns#_(.+)$~', $p, $m)) {
                        $graph->add_literal_triple($p, RDFS_LABEL, 'Item ' . $m[1]);
                        $labelled_properties[$p] = 1;
                    }
                }
            }
        }
    }
}
