<?php

/**
 * Utility class for labelling properties
 * Based on Moriarty's Labeller
 * @see https://code.google.com/p/moriarty/source/browse/trunk/labeller.class.php
 */
class Labeller {
    protected $_label_properties = array();

    protected $_ns = array (
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
    );
    var $_labels = array(
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_1' => array('first', 'first', 'is first member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_2' => array('second', 'second', 'is second member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_3' => array('third', 'third', 'is third member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_4' => array('fourth', 'fourth', 'is fourth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_5' => array('fifth', 'fifth', 'is fifth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_6' => array('sixth', 'sixth', 'is sixth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_7' => array('seventh', 'seventh', 'is seventh member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_8' => array('eighth', 'eighth', 'is eighth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_9' => array('ninth', 'ninth', 'is ninth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_10' => array('tenth', 'tenth', 'is tenth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_11' => array('eleventh', 'eleventh', 'is eleventh member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_12' => array('twelth', 'twelth', 'is twelth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_13' => array('thirteenth', 'thirteenth', 'is thirteenth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_14' => array('fourteenth', 'fourteenth', 'is fourteenth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_15' => array('fifteenth', 'fifteenth', 'is fifteenth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_16' => array('sixteenth', 'sixteenth', 'is sixteenth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_17' => array('seventeenth', 'seventeenth', 'is seventeenth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_18' => array('eighteenth', 'eighteenth', 'is eighteenth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_19' => array('nineteenth', 'nineteenth', 'is nineteenth member of'),
        'http://www.w3.org/1999/02/22-rdf-syntax-ns#_20' => array('twentieth', 'twentieth', 'is twentieth member of'),

        'http://www.w3.org/2000/01/rdf-schema#seeAlso' => array('see also','see also','is see also of'),
        'http://www.w3.org/2000/01/rdf-schema#isDefinedBy' => array('defined by','defined by','defines'),
        'http://www.w3.org/2000/01/rdf-schema#subClassOf' => array('subclass of','subclass of','is superclass of'),

        'http://www.w3.org/2002/07/owl#imports' => array('imports','imports','is imported by'),
        'http://www.w3.org/2002/07/owl#sameAs' => array('same as','same as','same as'),

        'http://xmlns.com/foaf/0.1/isPrimaryTopicOf' => array('is the primary topic of','is the primary topic of','primary topic'),
        'http://xmlns.com/foaf/0.1/primaryTopic' => array('primary topic','primary topics','is the primary topic of'),
        'http://xmlns.com/foaf/0.1/weblog' => array('blog','blogs','is weblog of'),
        'http://xmlns.com/foaf/0.1/knows' => array('knows','knows','knows'),
        'http://xmlns.com/foaf/0.1/firstName' => array('first name','first names','is first name of'),
        'http://xmlns.com/foaf/0.1/depiction' => array('picture','pictures','is picture of'),
        'http://xmlns.com/foaf/0.1/nick' => array('nickname','nickname','is nickname of'),
        'http://xmlns.com/foaf/0.1/phone' => array('phone number' ),
        'http://xmlns.com/foaf/0.1/mbox' => array('email address' ),
        'http://xmlns.com/foaf/0.1/workplaceHomepage' => array('workplace\'s homepage' ),
        'http://xmlns.com/foaf/0.1/schoolHomepage' => array('school\'s homepage' ),
        'http://xmlns.com/foaf/0.1/openid' => array('OpenID' ),
        'http://xmlns.com/foaf/0.1/mbox_sha1sum' => array('email address hashcode' ),
        'http://xmlns.com/foaf/0.1/made' => array('made', 'made', 'maker' ),
        'http://xmlns.com/foaf/0.1/accountProfilePage' => array('account profile page'),
        'http://xmlns.com/foaf/0.1/accountName' => array('account name'),
        'http://xmlns.com/foaf/0.1/accountServiceHomepage' => array('account service homepage'),
        'http://xmlns.com/foaf/0.1/holdsAccount' => array('account', 'accounts', 'is account held by'),

        'http://rdfs.org/sioc/ns#content' => array('content'),
        'http://rdfs.org/sioc/ns#hasContainer' => array('container'),


        'http://purl.org/dc/elements/1.1/identifier' => array('general identifier','general identifiers','is general identifier of'),
        'http://purl.org/dc/elements/1.1/type' => array('document type','document types','is document type of'),
        'http://purl.org/dc/elements/1.1/rights' => array('rights statement','right statements','is rights statement for'),

        'http://purl.org/dc/terms/abstract' => array('abstract','abstracts','is abstract of'),
        'http://purl.org/dc/terms/accessRights' => array('access rights','access rights','are access rights for'),
        'http://purl.org/dc/terms/alternative' => array('alternative title','alternative titles','is alternative title for'),
        'http://purl.org/dc/terms/audience' => array('audience','audiences','is audience for'),
        'http://purl.org/dc/terms/available' => array('date available','dates available','is date available of'),
        'http://purl.org/dc/terms/bibliographicCitation' => array('bibliographic citation','bibliographic citations','is bibliographic citation of'),
        'http://purl.org/dc/terms/contributor' => array('contributor','contributors','is contributor to'),
        'http://purl.org/dc/terms/coverage' => array('coverage','coverage','is coverage of'),
        'http://purl.org/dc/terms/created' => array('date created','dates created','is date created of'),
        'http://purl.org/dc/terms/creator' => array('creator','creators','is creator of'),
        'http://purl.org/dc/terms/date' => array('date','dates','is date of'),
        'http://purl.org/dc/terms/dateAccepted' => array('date accepted','dates accepted','is date accepted of'),
        'http://purl.org/dc/terms/dateCopyrighted' => array('date copyrighted','dates copyrighted','is date copyrighted of'),
        'http://purl.org/dc/terms/dateSubmitted' => array('date submitted','dates submitted','is date submitted of'),
        'http://purl.org/dc/terms/description' => array('description','descriptions','is description of'),
        'http://purl.org/dc/terms/format' => array('format','formats','is format of'),
        'http://purl.org/dc/terms/hasPart' => array('has part','has parts','is part of'),
        'http://purl.org/dc/terms/hasVersion' => array('version','versions','version of'),
        'http://purl.org/dc/terms/identifier' => array('identifier','identifiers','is identifier of'),
        'http://purl.org/dc/terms/isPartOf' => array('part of','part of','part'),
        'http://purl.org/dc/terms/isReferencedBy' => array('is referenced by','is referenced by','references'),
        'http://purl.org/dc/terms/isReplacedBy' => array('is replaced by','is replaced by','replaces'),
        'http://purl.org/dc/terms/isRequiredBy' => array('is required by','is required by','requires'),
        'http://purl.org/dc/terms/issued' => array('date issued','dates issued','is date issued of'),
        'http://purl.org/dc/terms/isVersionOf' => array('version of','version of','version'),
        'http://purl.org/dc/terms/language' => array('language','languages','is language of'),
        'http://purl.org/dc/terms/medium' => array('medium','media','is medium of'),
        'http://purl.org/dc/terms/modified' => array('date modified','dates modified','is date modified of'),
        'http://purl.org/dc/terms/replaces' => array('replaces','replaces','replaced by'),
        'http://purl.org/dc/terms/references' => array('references','references','is referenced by'),
        'http://purl.org/dc/terms/replaces' => array('replaces','replaces','is replaced by'),
        'http://purl.org/dc/terms/requires' => array('requires','requires','is required by'),
        'http://purl.org/dc/terms/rights' => array('rights statement','right statements','is rights statement for'),
        'http://purl.org/dc/terms/rightsHolder' => array('rights holder','rights holders','is rights holder of'),
        'http://purl.org/dc/terms/tableOfContents' => array('table of contents','tables of contents','is table of contents of'),
        'http://purl.org/dc/terms/type' => array('document type','document types','is document type of'),
        'http://purl.org/dc/terms/updated' => array('date updated','dates updated','is date updated of'),
        'http://purl.org/dc/terms/valid' => array('date valid','dates valid','is date valid of'),

        'http://www.w3.org/2003/01/geo/wgs84_pos#lat' => array('latitude','latitudes','is latitude of'),
        'http://www.w3.org/2003/01/geo/wgs84_pos#long' => array('longitude','longitudes','is longitude of'),

        'http://purl.org/vocab/bio/0.1/olb' => array('one line bio','one line bios','is one line bio of'),
        'http://purl.org/vocab/bio/0.1/event' => array('life event','life events','is life event of'),

        'http://purl.org/vocab/relationship/parentOf' => array('is parent of','is parent of','is child of'),
        'http://purl.org/vocab/relationship/childOf' => array('is child of','is child of','is parent of'),
        'http://purl.org/vocab/relationship/spouseOf' => array('spouse','spouses','spouse'),
        'http://purl.org/vocab/relationship/acquaintanceOf' => array('acquaintance'),
        'http://purl.org/vocab/relationship/friendOf' => array('friend'),

        'http://purl.org/vocab/vann/example' => array('example','examples','is example for'),
        'http://purl.org/vocab/vann/preferredNamespacePrefix' => array('preferred namespace prefix','preferred namespace prefixes','is preferred namespace prefix for'),
        'http://purl.org/vocab/vann/preferredNamespaceUri' => array('preferred namespace URI','preferred namespace URIs','is preferred namespace URI for'),
        'http://purl.org/vocab/vann/changes' => array('change log','change logs','is change log of'),

        'http://www.w3.org/2004/02/skos/core#prefLabel' => array('preferred label','preferred labels','is preferred label of'),
        'http://www.w3.org/2004/02/skos/core#altLabel' => array('alternative label','alternative labels','is alternative label of'),
        'http://www.w3.org/2004/02/skos/core#hiddenLabel' => array('hidden label','hidden labels','is hidden label of'),
        'http://www.w3.org/2004/02/skos/core#related' => array('related concept','related concepts','is related concept of'),
        'http://www.w3.org/2004/02/skos/core#broader' => array('broader concept','broader concepts','narrower concept'),
        'http://www.w3.org/2004/02/skos/core#narrower' => array('narrower concept','narrower concepts','broader concept'),
        'http://www.w3.org/2004/02/skos/core#scopeNote' => array('scope note','scope notes','is scope note of'),
        'http://www.w3.org/2004/02/skos/core#example' => array('example','examples','is example of'),
        'http://www.w3.org/2004/02/skos/core#historyNote' => array('history note','history notes','is history note of'),
        'http://www.w3.org/2004/02/skos/core#editorialNote' => array('editorial note','editorial notes','is editorial note of'),
        'http://www.w3.org/2004/02/skos/core#changeNote' => array('change note','change notes','is change note of'),
        'http://www.w3.org/2004/02/skos/core#inScheme' => array('scheme','schemes','is scheme of'),
        'http://www.w3.org/2004/02/skos/core#hasTopConcept' => array('top concept','top concepts','is top concept of'),
        'http://www.w3.org/2004/02/skos/core#exactMatch' => array('exact match','exact matches','is exact match of'),
        'http://www.w3.org/2004/02/skos/core#closeMatch' => array('close match','close matches','is close match of'),
        'http://www.w3.org/2004/02/skos/core#broadMatch' => array('broad match','broad matches','is broad match of'),
        'http://www.w3.org/2004/02/skos/core#narrowMatch' => array('narrow match','narrow matches','is narrow match of'),
        'http://www.w3.org/2004/02/skos/core#relatedMatch' => array('related match','related matches','is related match of'),

        'http://rdfs.org/ns/void#exampleResource' => array('example resource','example resources','is example resource of'),
        'http://rdfs.org/ns/void#sparqlEndpoint' => array('SPARQL endpoint','SPARQL endpoints','is SPARQL endpoint of'),
        'http://rdfs.org/ns/void#uriLookupEndpoint' => array('URI lookup point','URI lookup points','is URI lookup point of'),
        'http://rdfs.org/ns/void#dataDump' => array('data dump','data dumps','is data dump of'),
        'http://rdfs.org/ns/void#vocabulary' => array('vocabulary used','vocabularies used','is vocabulary used in'),
        'http://rdfs.org/ns/void#uriRegexPattern' => array('URI regex pattern'),

        'http://open.vocab.org/terms/numberOfPages' => array('number of pages','numbers of pages','is number of pages of'),
        'http://open.vocab.org/terms/subtitle' => array('sub-title','sub-titles','is sub-title of'),
        'http://open.vocab.org/terms/firstSentence' => array('first sentence'),
        'http://open.vocab.org/terms/category' => array('category', 'categories'),

        'http://purl.org/ontology/bibo/pageStart' => array('first page'),
        'http://purl.org/ontology/bibo/pageEnd' => array('last page'),
        'http://purl.org/ontology/bibo/issn' => array('ISSN','ISSNs','is ISSN of'),
        'http://purl.org/ontology/bibo/eissn' => array('EISSN','EISSNs','is EISSN of'),
        'http://purl.org/ontology/bibo/isbn' => array('ISBN','ISBNs','is ISBN of'),
        'http://purl.org/ontology/bibo/isbn10' => array('10 digit ISBN','10 digit ISBNs','is 10 digit ISBN of'),
        'http://purl.org/ontology/bibo/isbn13' => array('13 digit ISBN','13 digit ISBNs','is 13 digit ISBN of'),
        'http://purl.org/ontology/bibo/lccn' => array('LCCN','LCCNs','is LCCN of'),
        'http://purl.org/ontology/bibo/doi' => array('DOI','DOIs','is DOI of'),
        'http://purl.org/ontology/bibo/oclcnum' => array('OCLC number','OCLC numbers','is OCLC number of'),
        'http://purl.org/ontology/bibo/contributorList' => array('list of contributors','lists of contributors','is list of contributors to'),
        'http://purl.org/ontology/bibo/authorList' => array('list of authors','lists of authors','is list of authors of'),


        'http://purl.org/ontology/mo/wikipedia' => array('wikipedia page','wikipedia pages','is wikipedia page of'),
        'http://purl.org/ontology/mo/discogs' => array('discogs page','discogs pages','is discogs page of'),

        'http://purl.org/ontology/po/series' => array('series','series'),
        'http://purl.org/ontology/po/medium_synopsis' => array('medium synopsis','medium synopses'),
        'http://purl.org/ontology/po/short_synopsis' => array('short synopsis','short synopses'),
        'http://purl.org/ontology/po/long_synopsis' => array('long synopsis','long synopses'),
        'http://purl.org/ontology/po/format' => array('programme format'),
        'http://purl.org/ontology/po/masterbrand' => array('master  brand'),

        'http://purl.org/net/schemas/space/actor' => array('actor','actors','performed'),
        'http://purl.org/net/schemas/space/performed' => array('performed','performed','actor'),
        'http://purl.org/net/schemas/space/missionRole' => array('mission role'),
        'http://purl.org/net/schemas/space/alternateName' => array('alternate name'),
        'http://purl.org/net/schemas/space/mass' => array('mass'),
        'http://purl.org/net/schemas/space/spacecraft' => array('spacecraft', 'spacecraft'),
        'http://purl.org/net/schemas/space/agency' => array('agency'),
        'http://purl.org/net/schemas/space/launch' => array('launch', 'launches'),
        'http://purl.org/net/schemas/space/launchvehicle' => array('launch vehicle'),
        'http://purl.org/net/schemas/space/launchsite' => array('launch site'),
        'http://purl.org/net/schemas/space/launched' => array('launched', 'launched'),
        'http://purl.org/net/schemas/space/country' => array('country', 'countries'),

        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#borders' => array('borders', 'borders', 'borders'),
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasCensusCode' => array('census code'),
//      'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasArea' => array('area'),
//      'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasName' => array('name'),
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasOfficialName' => array('official name'),
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasOfficialWelshName' => array('official welsh name'),
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasVernacularName' => array('vernacular name'),
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#hasBoundaryLineName' => array('boundary line name'),
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#completelySpatiallyContains' => array('completely spatially contains',  'completely spatially contains',  'is completely spatially contained by'),
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#tangentiallySpatiallyContains' => array('tangentially spatially contains',  'tangentially spatially contains',  'is tangentially spatially contained by'),
        'http://www.ordnancesurvey.co.uk/ontology/AdministrativeGeography/v2.0/AdministrativeGeography.rdf#isSpatiallyEqualTo' => array('spatially equal to',  'spatially equal to',  'spatially equal to'),

        'http://rdvocab.info/Elements/placeOfPublication' => array('place of publication',  'places of publication'),

        'http://www.w3.org/2000/10/swap/pim/contact#nearestAirport' => array('nearest airport'),

        'http://www.daml.org/2001/10/html/airport-ont#icao' => array('ICAO', 'ICAOs', 'is ICAO of'),
        'http://www.daml.org/2001/10/html/airport-ont#iata' => array('IATA', 'IATAs', 'is IATA of'),

        'http://schemas.talis.com/2005/address/schema#regionName' => array('region name'),
        'http://schemas.talis.com/2005/address/schema#streetAddress' => array('street address'),
        'http://schemas.talis.com/2005/address/schema#localityName' => array('locality name'),
        'http://schemas.talis.com/2005/address/schema#postalCode' => array('postal code'),

        'http://schemas.talis.com/2006/recordstore/schema#tags' => array('tag'),
        'http://schemas.talis.com/2006/recordstore/schema#changeReason' => array('reason for change', 'reasons for change'),
        'http://schemas.talis.com/2006/recordstore/schema#active' => array('is active?', 'is active?'),
        'http://schemas.talis.com/2006/recordstore/schema#createdDate' => array('date created', 'dates created'),
        'http://schemas.talis.com/2006/recordstore/schema#previousState' => array('previous state'),
        'http://schemas.talis.com/2006/recordstore/schema#appliedBy' => array('applied by','applied by'),
        'http://schemas.talis.com/2006/recordstore/schema#appliedDate' => array('date applied','dates applied'),
        'http://schemas.talis.com/2006/recordstore/schema#reason' => array('reason'),

        'http://schemas.talis.com/2005/dir/schema#etag' => array('ETag'),

        'http://www.gazettes-online.co.uk/ontology#isAbout' => array('about', 'about'),
        'http://www.gazettes-online.co.uk/ontology#isInIssue' => array('issue'),
        'http://www.gazettes-online.co.uk/ontology/location#hasAddress' => array('address', 'addresses'),
        'http://www.gazettes-online.co.uk/ontology/court#courtName' => array('court name'),
        'http://www.gazettes-online.co.uk/ontology/court#sitsAt' => array('sits at', 'sits at'),

        'http://www.geonames.org/ontology#parentFeature' => array('parent feature'),

        'http://purl.org/goodrelations/v1#hasEAN_UCC-13' => array('EAN-13'),
        'http://purl.org/goodrelations/v1#hasMPN' => array('Manufacturer Part Number'),
        'http://purl.org/goodrelations/v1#hasGTIN-14' => array('GTIN-14'),
    );


    /**
     * Map a portion of a URI to a short prefix for use when serialising the graph
     * @param string prefix the namespace prefix to associate with the URI
     * @param string uri the URI to associate with the prefix
     */
    function set_namespace_mapping($prefix, $uri) {
        $this->_ns[$prefix] = $uri;
    }

    /**
     * Convert a QName to a URI using registered namespace prefixes
     * @param string qname the QName to convert
     * @return string the URI corresponding to the QName if a suitable prefix exists, null otherwise
     */
    function qname_to_uri($qname) {
        if (preg_match("~^(.+):(.+)$~", $qname, $m)) {
            if ( isset($this->_ns[$m[1]])) {
                return $this->_ns[$m[1]] . $m[2];
            }
        }

        return null;
    }

    /**
     * Convert a URI to a QName using registered namespace prefixes
     * @param string uri the URI to convert
     * @return string the QName corresponding to the URI if a suitable prefix exists, null otherwise
     */
    function uri_to_qname($uri) {
        if (preg_match('~^(.*[\/\#])([a-z0-9\-\_\:]+)$~i', $uri, $m)) {
            $ns = $m[1];
            $localname = $m[2];
            $prefix = $this->get_prefix($ns);
            if ( $prefix != null && $prefix !== FALSE) {
                return $prefix . ':' . $localname;
            }
        }
        return null;
    }

    function get_prefix($ns) {
        $prefix = array_search($ns, $this->_ns);
        if ( $prefix != null && $prefix !== FALSE) {
            return $prefix;
        }
        else {
            $parts = preg_split('/[\/#]/', $ns);
            for ($i = count($parts) - 1; $i >= 0; $i--) {
                if (preg_match('~^[a-zA-Z][a-zA-Z0-9\-]+$~', $parts[$i]) && !array_key_exists($parts[$i], $this->_ns) && $parts[$i] != 'schema' && $parts[$i] != 'ontology' && $parts[$i] != 'vocab' && $parts[$i] != 'terms' && $parts[$i] != 'ns' && $parts[$i] != 'core' && strlen($parts[$i]) > 3) {
                    $prefix = strtolower($parts[$i]);
                    $this->_ns[$prefix] = $ns;
                    return $prefix;
                }
            }

            $index = 0;
            while (array_key_exists('ns' . $index, $this->_ns)) {
                $index++;
            }
            $prefix = 'msg' . $index;
            $this->_ns[$prefix] = $ns;
            return $prefix;


        }
        return $prefix;
    }

    function add_labelling_property($p)  {
        $this->_label_properties[] = $p;
    }

    function get_ns() {
        return $this->_ns;
    }

    function get_label($uri, $g = null, $capitalize = false, $use_qnames = FALSE) {
        if ($g) {
            $label = $g->get_first_literal($uri,'http://www.w3.org/2004/02/skos/core#prefLabel', '', 'en');
            if ( strlen($label) != 0) return $label;

            $label = $g->get_first_literal($uri,RDFS_LABEL, '', 'en');
            if ( strlen($label) != 0) return $label;

            $label = $g->get_first_literal($uri,'http://purl.org/dc/terms/title', '', 'en');
            if ( strlen($label) != 0) return $label;

            $label = $g->get_first_literal($uri,DC_TITLE, '', 'en');
            if ( strlen($label) != 0) return $label;

            $label = $g->get_first_literal($uri,FOAF_NAME, '', 'en');
            if ( strlen($label) != 0) return $label;

            $label = $g->get_first_literal($uri,'http://www.geonames.org/ontology#name', '', 'en');
            if ( strlen($label) != 0) return $label;

            $label = $g->get_first_literal($uri,RDF_VALUE, '', 'en');
            if ( strlen($label) != 0) return $label;

            $label = $g->get_first_literal($uri,'http://purl.org/rss/1.0/title', '', 'en');
            if ( strlen($label) != 0) return $label;

            foreach ($this->_label_properties as $p) {
                $label = $g->get_first_literal($uri,$p, '', 'en');
                if ( strlen($label) != 0) return $label;
            }


        }

        if (array_key_exists($uri, $this->_labels)) {
            if ($capitalize) {
                return ucfirst($this->_labels[$uri][0]);
            }
            else {
                return $this->_labels[$uri][0];
            }
        }
        else if (preg_match('~^http://www.w3.org/1999/02/22-rdf-syntax-ns#_(.+)$~', $uri, $m)) {
            if ($capitalize) {
                return 'Item ' . $m[1];
            }
            else {
                return 'item ' . $m[1];
            }
        }
        else {
            if ($use_qnames) {
                $label = $this->uri_to_qname($uri);
                if ($label) return $label;
            }
            else {
                if (preg_match('~^.*[\/\#]([^\/\#]+)$~', $uri, $m)) {
                    $localname = $m[1];
                    if (preg_match('~[^A-Z][A-Z][^A-Z]~', $localname)) {
                        $parts = preg_split('/([A-Z][^A-Z]*)/', $localname, -1, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
                        $parts = array_map('strtolower', $parts);
                        if ($parts[0] == 'has') {
                            array_shift($parts);
                        }
                        $label = join(' ', $parts);
                        if ($capitalize) {
                            return ucfirst($label);
                        }
                        else {
                            return $label;
                        }
                    }
                    else {
                        if ($capitalize && preg_match('~^[a-z]~', $localname)) {
                            return ucfirst($localname);
                        }
                        else {
                            return $localname;
                        }
                    }
                }
            }
        }
        return $uri;
    }


    function get_plural_label($uri, $g = null, $capitalize = false, $use_qnames = FALSE) {
        if ($g) {
            $label = $g->get_first_literal($uri,'http://purl.org/net/vocab/2004/03/label#plural', '', 'en');
            if ( strlen($label) != 0) return $label;
        }

        if (array_key_exists($uri, $this->_labels)) {
            if (count($this->_labels[$uri]) > 1) {
                $label = $this->_labels[$uri][1];
            }
            else {
                $label = $this->_labels[$uri][0] . 's';
            }
            if ($capitalize) {
                return ucfirst($label);
            }
            else {
                return $label;
            }
        }
        else if ($use_qnames == FALSE && preg_match('~^.*[\/\#]([a-z]+)$~', $uri, $m)) {
            return $m[1] . 's';
        }


        $label = $this->get_label($uri, $g, $capitalize, $use_qnames);
        if (preg_match('~^[^\s]+$~', $label)) {
            return $label . 's';
        }
        else {
            return $label;
        }
    }

    function get_inverse_label($uri, $g = null, $capitalize = false, $use_qnames = FALSE) {
        if ($g) {
            $label = $g->get_first_literal($uri,'http://purl.org/net/vocab/2004/03/label#inverseSingular', '', 'en');
            if ( strlen($label) != 0) return $label;
        }

        if (array_key_exists($uri, $this->_labels)) {
            if (count($this->_labels[$uri]) > 2) {
                $label = $this->_labels[$uri][2];
            }
            else {
                $label = 'is ' . $this->_labels[$uri][0] . ' of';
            }
            if ($capitalize) {
                return ucfirst($label);
            }
            else {
                return $label;
            }
        }

        $label = 'is ' . $this->get_label($uri, $g, false, $use_qnames) . ' of';
        if ($capitalize) {
            return ucfirst($label);
        }
        else {
            return $label;
        }

    }


    function label_graph(&$graph) {
        $labelled_properties = array();
        $index = $graph->get_index();
        foreach ($index as $s => $p_list) {
            foreach ($p_list as $p => $val) {
                if ( !array_key_exists($p, $labelled_properties) ) {

                    if ( array_key_exists($p, $this->_labels) ) {
                        if ( ! $graph->subject_has_property($p, RDFS_LABEL) ) {
                            $graph->add_literal_triple($p, RDFS_LABEL, $this->_labels[$p][0]);
                        }
                        if ( ! $graph->subject_has_property($p, 'http://purl.org/net/vocab/2004/03/label#plural') ) {
                            if (count($this->_labels[$p]) > 1 ) {
                                $graph->add_literal_triple($p, 'http://purl.org/net/vocab/2004/03/label#plural', $this->_labels[$p][1]);
                            }
                            else {
                                $graph->add_literal_triple($p, 'http://purl.org/net/vocab/2004/03/label#plural', $this->_labels[$p][0] . 's');
                            }
                        }
                        if ( ! $graph->subject_has_property($p, 'http://purl.org/net/vocab/2004/03/label#inverseSingular') ) {
                            if (count($this->_labels[$p]) > 2 ) {
                                $graph->add_literal_triple($p, 'http://purl.org/net/vocab/2004/03/label#inverseSingular', $this->_labels[$p][2]);
                            }
                            else {
                                $graph->add_literal_triple($p, 'http://purl.org/net/vocab/2004/03/label#inverseSingular', 'is ' . $this->_labels[$p][0] . ' of');
                            }
                        }
                        $labelled_properties[$p] = 1;
                    }
                    else if (preg_match('~^http://www.w3.org/1999/02/22-rdf-syntax-ns#_(.+)$~', $p, $m)) {
                        $graph->add_literal_triple($p, RDFS_LABEL, 'Item ' . $m[1]);
                        $labelled_properties[$p] = 1;
                    }
                }
            }
        }


    }

}
