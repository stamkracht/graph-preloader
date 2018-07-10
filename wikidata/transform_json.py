#! /usr/bin/env python3

"""transform_json.py

takes as input json objects in wikidata format (one object per line), and
outputs json vertices and edges usable as input to DSE graph loader.
"""

import itertools
import json
import sys

import properties

COPY_PROPERTIES = {'id', 'modified', 'type', 'title', 'lastrevid'}


def iter_claims(entity):
    if not entity['claims']:
        return []
    return itertools.chain.from_iterable(entity['claims'].values())


def transform_edge(item_id, snak):
    return {
        'id': snak['property'],
        'outV': item_id,
        'inV': snak['datavalue']['value']['id']
    }


def english_or_default(langs):
    if not langs:
        return None
    if 'en' in langs:
        return langs['en']['value']
    else:
        return langs.popitem()[1]['value']


def transform(entity):
    edges = []
    transformed = {
        key: value for key, value in entity.items()
        if key in COPY_PROPERTIES
    }

    transformed['label'] = english_or_default(entity['labels'])
    transformed['description'] = english_or_default(entity['descriptions'])

    for claim in iter_claims(entity):
        snak = claim.get('mainsnak')
        if not snak or snak['snaktype'] != 'value':
            continue

        if snak['datatype'] == 'wikibase-item':
            edges.append(transform_edge(entity['id'], snak))
        else:
            property_id = snak['property']
            value = properties.get_value(snak['datavalue'])
            transformed[property_id] = value

    return transformed, edges


def main():
    vertex_file = open('data/dse_entities.dump', 'w')
    edge_file = open('data/dse_edges.dump', 'w')

    for line in sys.stdin:
        entity = json.loads(line)
        print('transforming entity {}'.format(entity['id']))

        try:
            transformed, edges = transform(entity)
        except Exception:
            import pprint
            import traceback
            traceback.print_exc()
            pprint.pprint(entity)
            sys.exit(1)
        print(json.dumps(transformed), file=vertex_file)
        for edge in edges:
            print(json.dumps(edge), file=edge_file)


if __name__ == "__main__":
    main()
