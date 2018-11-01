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
    'wikibase-lexeme': 'Text',
}


def get_type(datatype):
    if not isinstance(datatype, str):
        print('found strange datatype:', datatype)
        sys.exit(-1)
    return types_mapping.get(datatype)


def main():
    for line in sys.stdin:
        entity = json.loads(line)

        if entity['id'][0] == 'Q':
            continue
        if 'datatype' not in entity:
            print(f'entity has no datatype:\n{entity["id"]}', file=sys.stderr)
            continue

        t = get_type(entity['datatype'])

        if t == 'Item':
            schema = "edgeLabel('{id}').connection('item', 'item').create()"
        elif t == 'Property':
            schema = "edgeLabel('{id}').connection('item', 'property').create()"
        elif t is None:
            print(f"Unknown type '{entity['datatype']}' for entity {entity['id']}", file=sys.stderr)
        else:
            schema = "schema.propertyKey('{id}').{type}().create()"

        print(schema.format(id=entity['id'], type=t))


if __name__ == "__main__":
    main()
