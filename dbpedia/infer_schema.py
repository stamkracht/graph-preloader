#! /usr/bin/env python3

import itertools
import collections
import sys

DBPEDIA_TYPE_URI = 'http://dbpedia.org/datatype/'

CURRENCY_TYPES = set(DBPEDIA_TYPE_URI + t for t in [
    "algerianDinar", "argentinePeso", "armenianDram", "australianDollar",
    "azerbaijaniManat", "bahrainiDinar", "bangladeshiTaka", "belarussianRuble",
    "belizeDollar", "bosniaAndHerzegovinaConvertibleMarks", "botswanaPula",
    "brazilianReal", "bulgarianLev", "canadianDollar", "capeVerdeEscudo",
    "centralAfricanCfaFranc", "cfpFranc", "chileanPeso", "colombianPeso",
    "comorianFranc", "croatianKuna", "cubicMetre", "czechKoruna",
    "danishKrone", "egyptianPound", "estonianKroon", "ethiopianBirr", "euro",
    "gambianDalasi", "georgianLari", "ghanaianCedi", "gibraltarPound",
    "guineaFranc", "haitiGourde", "honduranLempira", "hongKongDollar",
    "hungarianForint", "icelandKrona", "indianRupee", "indonesianRupiah",
    "iranianRial", "iraqiDinar", "jamaicanDollar", "japaneseYen",
    "jordanianDinar", "kazakhstaniTenge", "kenyanShilling", "kuwaitiDinar",
    "latvianLats", "lithuanianLitas", "macedonianDenar", "malawianKwacha",
    "malaysianRinggit", "maldivianRufiyaa", "mauritianRupee", "mexicanPeso",
    "moldovanLeu", "moroccanDirham", "myanmaKyat", "namibianDollar",
    "nepaleseRupee", "netherlandsAntilleanGuilder", "newTaiwanDollar",
    "newZealandDollar", "nicaraguanCórdoba", "nigerianNaira", "norwegianKrone",
    "omaniRial", "pakistaniRupee", "papuaNewGuineanKina", "peruvianNuevoSol",
    "philippinePeso", "polishZłoty", "poundSterling", "qatariRial", "renminbi",
    "romanianNewLeu", "russianRouble", "rwandaFranc", "saudiRiyal",
    "serbianDinar", "sierraLeoneanLeone", "singaporeDollar", "slovakKoruna",
    "southAfricanRand", "southKoreanWon", "sriLankanRupee", "surinamDollar",
    "swedishKrona", "swissFranc", "tanzanianShilling", "thaiBaht",
    "trinidadAndTobagoDollar", "tunisianDinar", "turkishLira",
    "ugandaShilling", "ukrainianHryvnia", "unitedArabEmiratesDirham",
    "usDollar", "uzbekistanSom", "westAfricanCfaFranc", "zambianKwacha",
])

DSE_TYPE_MAP = {
    "string": "Text",
    "http://dbpedia.org/datatype/Area": "Double",
    "http://dbpedia.org/datatype/Currency": "Float",
    "http://dbpedia.org/datatype/Length": "Double",
    "http://dbpedia.org/datatype/Mass": "Double",
    "http://dbpedia.org/datatype/Power": "Double",
    "http://dbpedia.org/datatype/Time": "Duration",
    "http://dbpedia.org/datatype/engineConfiguration": "Text",
    "http://dbpedia.org/datatype/fuelType": "Text",
    "http://dbpedia.org/datatype/kilogram": "Double",
    "http://dbpedia.org/datatype/kilometre": "Double",
    "http://dbpedia.org/datatype/millimetre": "Double",
    "http://dbpedia.org/datatype/squareKilometre": "Double",
    "http://dbpedia.org/datatype/valvetrain": "Text",
    "http://www.w3.org/2001/XMLSchema#anyURI": "Text",
    "http://www.w3.org/2001/XMLSchema#boolean": "Boolean",
    "http://www.w3.org/2001/XMLSchema#date": "Date",
    "http://www.w3.org/2001/XMLSchema#double": "Double",
    "http://www.w3.org/2001/XMLSchema#float": "Float",
    "http://www.w3.org/2001/XMLSchema#gYear": "Date",
    "http://www.w3.org/2001/XMLSchema#gYearMonth": "Date",
    "http://www.w3.org/2001/XMLSchema#integer": "Int",
    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger": "Int",
    "http://www.w3.org/2001/XMLSchema#positiveInteger": "Int",
}


Property = collections.namedtuple(
    'Property',
    ['name', 'type', 'languages'],
    defaults=[tuple(),]
)


def is_lang(t):
    return t.startswith('@')


def read_types(typefile):
    types = map(lambda line: line.split(maxsplit=1))
    prop_groups = itertools.groupby(lambda g: g[0])

    for prop, statements in prop_groups:
        langs, types = [], []
        for x in statements:
            (langs if is_lang(x) else types).append(x)

        if len(types) == 1:
            yield Property(prop, types[0], langs)
        elif set(types) == set(('global_uri', 'external_uri')):
            # just say it's a global_uri if we have both
            yield Property(prop, 'global_uri', langs)
        else:
            print(f'weird set of types for {prop}: {types}', file=sys.stderr)


def make_edge(label, langs):
    props = ".properties('language')" if langs else ''
    return f"edgeLabel('{label}').connection('item', 'item'){props}.create()"


def make_property(label, datatype, langs):
    props = ".properties('language')" if langs else ''
    return f"schema.propertyKey('{prop.name}').{datatype}(){props}.create()"


def get_schema(properties):
    for prop in properties:
        if prop.type == 'global_uri':
            return make_edge(prop.name)

        if prop.type in CURRENCY_TYPES:
            dse_type = 'Double'
        else:
            dse_type = DSE_TYPE_MAP[prop.type]
        return make_property(prop.name, dse_type)


if __name__ == "__main__":
    props = read_types(sys.stdin)
    print("schema.propertyKey('language').Text().create()")
    for line in get_schema(props):
        print(line)
