#! /usr/bin/env python3

"""humble beginnings to create a property schema from wikidata property list"""

import sys
import json

import properties

# this property marks constraints
CONSTRAINTS_PROP = 'P2302'
# this property is a qualifier on the below one to indicate allowed property
PROPERTY_PROP = 'P2306'
# this entity value specifies allowed qualifiers in its qualifier (yeah I know)
ALLOWED_QUALIFIERS_ENTITY = 'Q21510851'

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


def get_edge_properties(prop):
    """qualifiers of properties are turned into edge properties

    This method looks at the allowed qualifiers constraint on the property to
    generate a schema.
    """
    def is_allowed_quals(claim):
        v = properties.get_value(claim['mainsnak']['datavalue'])
        return v == ALLOWED_QUALIFIERS_ENTITY

    constraints = prop['claims'][CONSTRAINTS_PROP]
    allowed_qualifiers = list(filter(is_allowed_quals, constraints))

    assert len(allowed_qualifiers) == 1
    allowed_qualifiers = allowed_qualifiers[0]['qualifiers'][PROPERTY_PROP]
    return [snak['datavalue']['value']['id'] for snak in allowed_qualifiers]


def get_property_schema(prop):
    eid = prop['id']
    etype = get_type(prop['datatype'])

    if etype in ('Item', 'Property'):
        etype = etype.lower()
        edge_properties = ', '.join(map(repr, get_edge_properties(prop)))
        schema = f"edgeLabel('{eid}').connection('item', '{etype}').properties({edge_properties}).create()"
    elif etype is None:
        print(f"Unknown type '{prop['datatype']}' for prop {prop['id']}", file=sys.stderr)
    else:
        schema = "schema.propertyKey('{eid}').{etype}().create()"
    return schema


def main():
    for line in sys.stdin:
        entity = json.loads(line)

        if entity['id'][0] == 'Q':
            continue
        if 'datatype' not in entity:
            print(f'entity has no datatype:\n{entity["id"]}', file=sys.stderr)
            continue

        schema = get_property_schema(entity)
        print(schema)


if __name__ == "__main__":
    main()
