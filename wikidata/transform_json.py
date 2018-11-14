#! /usr/bin/env python3

"""transform_json.py

takes as input json objects in wikidata format (one object per line), and
outputs json vertices and edges usable as input to DSE graph loader.
"""

import functools
import itertools
import json
import sys
import argparse
import multiprocessing
import requests

import properties

SAMETHING_SERVICE = None
COPY_PROPERTIES = {'modified', 'type', 'title', 'lastrevid'}


@functools.lru_cache(maxsize=4096)
def transform_id(item_id):
    wikidata_url = f'http://www.wikidata.org/entity/'
    r = requests.get(f'{SAMETHING_SERVICE}lookup/?uri={wikidata_url}{item_id}')
    if r.ok:
        return r.json()['global']
    else:
        print(f'same-thing: item id {item_id} was not found. Not transforming')
        return item_id


def iter_claims(entity):
    if not entity['claims']:
        return []
    return itertools.chain.from_iterable(entity['claims'].values())


def transform_edge(item_id, snak):
    return {
        'id': snak['property'],
        'outV': item_id,
        'inV': transform_id(snak['datavalue']['value']['id'])
    }


def add_qualifiers(edge, qualifiers):
    for q in qualifiers.values():
        prop = q['property']
        edge[prop] = properties.get_value(q['datavalue'])


def multilang_property(langs):
    if not langs:
        return None
    else:
        return list(langs.values())


def transform(entity):
    edges = []
    transformed = {
        key: value for key, value in entity.items()
        if key in COPY_PROPERTIES
    }

    transformed['id'] = transform_id(entity['id'])
    transformed['label'] = multilang_property(entity['labels'])
    transformed['description'] = multilang_property(entity['descriptions'])

    for claim in iter_claims(entity):
        snak = claim.get('mainsnak')
        if not snak or snak['snaktype'] != 'value':
            continue

        if snak['datatype'] == 'wikibase-item':
            edge = transform_edge(transformed["id"], snak)
            add_qualifiers(edge, claim.get('qualifiers'))
            edges.append(edge)
        else:
            property_id = snak['property']
            value = properties.get_value(snak['datavalue'])
            if claim.get('qualifiers'):
                prop = {'value': value}
                add_qualifiers(prop, claim['qualifiers'])
            else:
                prop = value
            transformed[property_id] = prop

    return transformed, edges


def process_line(line):
    try:
        entity = json.loads(line)
        transformed, edges = transform(entity)
        return transformed, edges, None
    except Exception:
        return None, None, sys.exc_info()


def get_map_func(is_parallel):
    if is_parallel:
        p = multiprocessing.Pool()
        return lambda i: p.imap_unordered(process_line, i, chunksize=20)
    else:
        return lambda i: map(process_line, i)


def main(args):
    global SAMETHING_SERVICE
    SAMETHING_SERVICE = args.samething

    vertex_file = open("data/dse_entities.dump", "w")
    edge_file = open("data/dse_edges.dump", "w")

    map_func = get_map_func(args.parallel)
    for transformed, edges, error in map_func(iter(sys.stdin)):
        if error is not None:
            import traceback
            traceback.print_exception(*error)
            sys.exit(1)

        print("transformed entity {}".format(transformed["id"]))
        print(json.dumps(transformed), file=vertex_file)
        for edge in edges:
            print(json.dumps(edge), file=edge_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parallel", action="store_true",
                        help="transform elements in parallel")
    parser.add_argument("--samething", action="store", metavar="URL",
                        default="https://e.hum.uva.nl/same-thing/",
                        help="URL to use when transforming IDs to global")
    main(parser.parse_args())
