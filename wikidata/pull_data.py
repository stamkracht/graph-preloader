#! /usr/bin/env python3

"""pull_data.py

pull a number of entities from wikidata and print the JSON output.

Usage:
    pull_data.py [options] <entity>
    pull_data.py [options] <start> <end>

Options:
    -h --help                    Show this message
    -d <depth>, --depth=<depth>  recursively crawl entities to depth. [default: 0]
"""

import sys
import requests
import json
import urllib.parse

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

    def __init__(self):
        self.request_queue = []
        self.result_queue = []

    def make_request(self):
        if not self.request_queue:
            raise FetcherError('nothing left to fetch')

        print('fetching {} ids'.format(len(self.request_queue)), file=sys.stderr)
        ids = self.request_queue[:50].join('|')
        req = urllib.parse.quote_plus(self.api_query.format(ids))
        resp = requests.get(self.base_url + req)

        if not resp.ok:
            raise FetcherError(resp.text)

        # add existing entities to result queue
        resp = resp.json()
        results = resp['entities'].values()
        not_missing = filter(lambda r: 'missing' not in r, results)
        self.result_queue.extend(not_missing)
        # remove what we fetched from request queue
        del self.request_queue[:50]

    def schedule(self, entity_id):
        self.request_queue.append(entity_id)
        while len(self.request_queue) >= 50:
            self.make_request()

    def get_next(self):
        if not self.result_queue:
            self.make_request()
        return self.result_queue.pop()


def fetch_entity(entity_id):
    url = 'http://www.wikidata.org/entity/{}'.format(entity_id)
    resp = requests.get(url, headers={'Accept': 'application/json'})
    if resp.ok:
        _, entity = resp.json()['entities'].popitem()
        return entity
    else:
        raise EntityError('error fetching entity {}: {}\n'.format(
            entity_id, resp.status_code))


def crawl_entity(entity):
    if not entity['claims']:
        return

    for key, claims in entity['claims'].items():
        yield key
        for claim in claims:
            if 'mainsnak' not in claim:
                continue
            snak = claim['mainsnak']
            if snak['datatype'] == 'wikibase-item' and snak['snaktype'] == 'value':
                yield claim['mainsnak']['datavalue']['value']['id']


def recursive_fetch(entity_id, max_depth, depth=0):
    sys.stderr.write('crawling entity {}\n'.format(entity_id))
    entity = fetch_entity(entity_id)
    yield entity
    fetched_entities.add(entity_id)
    if depth < max_depth:
        for sub_id in crawl_entity(entity):
            if sub_id in fetched_entities:
                continue
            yield from recursive_fetch(sub_id, max_depth, depth + 1)


def main(args):
    if args['<entity>']:
        start = int(args['<entity>'])
        end = start + 1
    else:
        start, end = int(args['<start>']), int(args['<end>'])
    depth = int(args['--depth'])
    success_count = 0
    errors = []

    for i in range(start, end):
        try:
            for entity in recursive_fetch('Q' + str(i), depth):
                print(json.dumps(entity))
                success_count += 1
        except EntityError as e:
            errors.append(str(e))

    sys.stderr.write('successfully fetched {} entities\n'.format(success_count))
    if errors:
        sys.stderr.write('The following errors were encountered:\n')
    for error in errors:
        sys.stderr.write(error)


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
