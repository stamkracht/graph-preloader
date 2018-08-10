#! /usr/bin/env python3

"""transform_json.py

takes as input json objects in wikidata format (one object per line), and
outputs json vertices and edges usable as input to DSE graph loader.
"""

import itertools
import json
import sys
import argparse
import multiprocessing

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


def process_line(line):
    try:
        entity = json.loads(line)
        transformed, edges = transform(entity)
        return transformed, edges, None
    except Exception as e:
        return None, None, e


def get_map_func(is_parallel):
    if is_parallel:
        p = multiprocessing.Pool()
        return lambda i: p.imap_unordered(process_line, i, chunksize=20)
    else:
        return lambda i: map(process_line, i)


def main(args):
    vertex_file = open('data/dse_entities.dump', 'w')
    edge_file = open('data/dse_edges.dump', 'w')

    map_func = get_map_func(args.parallel)
    for transformed, edges, error in map_func(iter(sys.stdin)):
        if error is not None:
            import traceback
            traceback.print_exc(error)
            sys.exit(1)

        print("transformed entity {}".format(transformed["id"]))
        print(json.dumps(transformed), file=vertex_file)
        for edge in edges:
            print(json.dumps(edge), file=edge_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parallel", action="store_true",
                        help="transform elements in parallel")
    main(parser.parse_args())
