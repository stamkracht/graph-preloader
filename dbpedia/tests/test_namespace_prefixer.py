import pytest

from dbpedia.graph_elements import NamespacePrefixer


def test_split_uri():
    prefixer = NamespacePrefixer()

    with pytest.raises(ValueError):
        case_unknown = prefixer.split_iri('http://unknown.namespace/Semantics')

    case_slash_1 = prefixer.split_iri('http://dbpedia.org/ontology/Taxon')
    assert case_slash_1 == ('http://dbpedia.org/ontology/', 'Taxon')

    case_slash_2 = prefixer.split_iri('http://dbpedia.org/resource/AC/DC')
    assert case_slash_2 == ('http://dbpedia.org/resource/', 'AC/DC')

    case_hashtag_1 = prefixer.split_iri('http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#Concept')
    assert case_hashtag_1 == ('http://www.ontologydesignpatterns.org/ont/dul/DUL.owl', 'Concept')

    case_hashtag_2 = prefixer.split_iri('http://www.geonames.org/ontology#Feature')
    assert case_hashtag_2 == ('http://www.geonames.org/ontology#', 'Feature')

    case_semicolon_1 = prefixer.split_iri('http://dbpedia.org/resource/Category:Life')
    assert case_semicolon_1 == ('http://dbpedia.org/resource/Category:', 'Life')

    case_semicolon_2 = prefixer.split_iri('http://dbpedia.org/resource/10:20:31/50')
    assert case_semicolon_2 == ('http://dbpedia.org/resource/', '10:20:31/50')


def test_qname():
    prefixer = NamespacePrefixer()

    case_unknown = prefixer.qname('http://unknown.namespace/Semantics')
    assert case_unknown == 'http://unknown.namespace/Semantics'

    case_slash_1 = prefixer.qname('http://dbpedia.org/ontology/Taxon')
    assert case_slash_1 == 'dbo:Taxon'

    case_slash_2 = prefixer.qname('http://dbpedia.org/resource/AC/DC')
    assert case_slash_2 == 'dbr:AC/DC'

    case_hashtag_1 = prefixer.qname('http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#Concept')
    assert case_hashtag_1 == 'dul:Concept'

    case_hashtag_2 = prefixer.qname('http://www.geonames.org/ontology#Feature')
    assert case_hashtag_2 == 'geonames:Feature'

    case_semicolon_1 = prefixer.qname('http://dbpedia.org/resource/Category:Life')
    assert case_semicolon_1 == 'dbc:Life'

    case_semicolon_2 = prefixer.qname('http://dbpedia.org/resource/10:20:31/50')
    assert case_semicolon_2 == 'dbr:10:20:31/50'


def test_reverse():
    prefixer = NamespacePrefixer()

    case_unknown = prefixer.reverse('unk:namespace/Semantics')
    assert case_unknown == 'unk:namespace/Semantics'

    case_slash_1 = prefixer.reverse('dbo:Taxon')
    assert case_slash_1 == 'http://dbpedia.org/ontology/Taxon'

    case_slash_2 = prefixer.reverse('dbr:AC/DC')
    assert case_slash_2 == 'http://dbpedia.org/resource/AC/DC'

    case_hashtag_1 = prefixer.reverse('dul:Concept')
    assert case_hashtag_1 == 'http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#Concept'

    case_hashtag_2 = prefixer.reverse('geonames:Feature')
    assert case_hashtag_2 == 'http://www.geonames.org/ontology#Feature'

    case_semicolon_1 = prefixer.reverse('dbc:Life')
    assert case_semicolon_1 == 'http://dbpedia.org/resource/Category:Life'

    case_semicolon_2 = prefixer.reverse('dbr:10:20:31/50')
    assert case_semicolon_2 == 'http://dbpedia.org/resource/10:20:31/50'
