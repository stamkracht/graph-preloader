#! /usr/bin/env python3

"""pull_data.py

pull a number of entities from wikidata and print the JSON output.

Usage:
    pull_data.py [options] <entity>
    pull_data.py [options] <start> <end>

Options:
    -h --help                    Show this message
    -p, --properties             crawl properties instead of entities
    -d <depth>, --depth=<depth>  recursively crawl entities to depth. [default: 0]
"""

import sys
import requests
import json

ENTITIES_FILE = 'data/fetched_entities'
fetched_entities = set()


class EntityError(Exception):
    pass


class FetcherError(Exception):
    pass


class Fetcher(object):
    """Fetcher -- submit entity ids and retrieve entities

    batches entity ids into larger requests, to improve efficiency
    """
    base_url = 'https://www.wikidata.org/w/api.php'
    api_query = '?format=json&action=wbgetentities&ids={}'

    def __init__(self, fetch_size=50):
        self.fetch_size = fetch_size
        self.request_queue = []
        self.result_queue = []

    def make_request(self):
        if not self.request_queue:
            raise FetcherError('nothing left to fetch')

        to_fetch = self.request_queue[:self.fetch_size]
        print(f'fetching {len(to_fetch)} ids', file=sys.stderr)
        ids = '|'.join(to_fetch)
        req = self.api_query.format(ids)
        resp = requests.get(self.base_url + req)

        if not resp.ok:
            raise FetcherError(resp.text)

        # add existing entities to result queue
        resp = resp.json()
        results = resp['entities'].values()
        not_missing = filter(lambda r: 'missing' not in r, results)
        self.result_queue.extend(not_missing)
        # remove what we fetched from request queue
        del self.request_queue[:self.fetch_size]

    def schedule(self, entity_id):
        self.request_queue.append(entity_id)

    def get_next(self):
        if not self.result_queue:
            self.make_request()
        return self.result_queue.pop()


class RecursiveFetcher(object):
    """RecursiveFetcher -- recursively crawl entities up to a given depth

    Uses the Fetcher for efficient entity retrieval
    """
    def __init__(self, initial_set, max_depth):
        self.max_depth = max_depth
        # this map keeps track of the depth where we encountered this entity
        self.depth_map = {}
        self.fetcher = Fetcher()
        self.schedule(initial_set, depth=0)

    def schedule(self, entity_ids, depth):
        for eid in entity_ids:
            self.depth_map[eid] = depth
            self.fetcher.schedule(eid)

    def crawl(self, entity):
        if not entity['claims']:
            return

        for key, claims in entity['claims'].items():
            yield key
            for claim in claims:
                if 'mainsnak' not in claim:
                    continue
                snak = claim['mainsnak']
                dtype, stype = snak['datatype'], snak['snaktype']
                if dtype == 'wikibase-item' and stype == 'value':
                    yield snak['datavalue']['value']['id']

    def __iter__(self):
        return self

    def __next__(self):
        try:
            entity = self.fetcher.get_next()
        except FetcherError:
            raise StopIteration

        depth = self.depth_map[entity['id']]
        if depth < self.max_depth:
            # subtract everything we've already fetched
            new_entities = set(self.crawl(entity)) - self.depth_map.keys()
            print(f'scheduling {len(new_entities)} in {entity["id"]}, depth={depth + 1}', file=sys.stderr)
            self.schedule(new_entities, depth + 1)
        return entity


def main(args):
    if args['<entity>']:
        start = int(args['<entity>'])
        end = start + 1
    else:
        start, end = int(args['<start>']), int(args['<end>'])
    depth = int(args['--depth'])
    success_count = 0
    prefix = 'P' if args['--properties'] else 'Q'

    initial_set = [prefix + str(i) for i in range(start, end)]
    for entity in RecursiveFetcher(initial_set, depth):
        print(json.dumps(entity))
        success_count += 1
    sys.stderr.write('successfully fetched {} entities\n'.format(success_count))


if __name__ == "__main__":
    with open(ENTITIES_FILE) as f:
        for line in f:
            fetched_entities.add(line.strip())

    import docopt
    try:
        args = docopt.docopt(__doc__, version='pull_data 0.1')
        main(args)
    except KeyboardInterrupt as e:
        sys.stderr.write('Received interrupt, exiting...\n')
        sys.exit(-1)
    finally:
        with open(ENTITIES_FILE, 'w') as f:
            for entity in fetched_entities:
                f.write(entity + '\n')
