#! /usr/bin/env python3

"""humble beginnings to create a property schema from wikidata property list"""

import sys
import json

types_mapping = {
    'commonsMedia': 'Text',
    'external-id': 'Text',
    'tabular-data': 'Text',
    'url': 'Text',

    'string': 'Text',
    'monolingualtext': 'Text',
    'math': 'Text',

    'geo-shape': 'Text',
    'globe-coordinate': 'Text',

    'quantity': 'Int',
    'time': 'Timestamp',

    'wikibase-item': 'Item',
    'wikibase-property': 'Property',
}


def get_type(datatype):
    if not isinstance(datatype, str):
        print('found strange datatype:', datatype)
        sys.exit(-1)
    return types_mapping[datatype]


def main():
    for line in sys.stdin:
        entity = json.loads(line)

        t = get_type(entity['datatype'])

        if t == 'Item':
            schema = "edgeLabel('{id}').connection('item', 'item').create()"
        else:
            schema = "schema.propertyKey('{id}').{type}().create()"

        print(schema.format(id=entity['id'], type=t))


if __name__ == "__main__":
    main()
