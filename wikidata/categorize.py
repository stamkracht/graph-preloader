#! /usr/bin/env python3

"""
experiments to automatically determine nice clusters to assign vertex labels
to for wikidata entities.
"""

import json
import tqdm

ENTITY_FILE = open('data/dse_entities.dump')


class Entity(object):
    __slots__ = ('entity_id', 'properties', 'complexity')

    def __init__(self, entity_id, properties):
        self.entity_id = entity_id
        self.properties = frozenset(properties)
        self.complexity = len(properties)

    @classmethod
    def from_json(cls, data):
        return cls(data['id'], data.keys())

    def to_cluster(self):
        return Cluster([self], self.properties)


class Cluster(object):
    __slots__ = ('entities', 'properties', 'complexity')

    def __init__(self, entities, properties):
        self.entities = entities
        self.properties = set(properties)
        self.complexity = len(self.properties)

    @classmethod
    def merge(cls, a, b):
        return cls(a.entities + b.entities, a.properties | b.properties)

    def add_entity(self, entity):
        self.entities.append(entity)
        self.properties |= entity.properties
        self.complexity = len(self.properties)


def entity_distance(x, y):
    max_properties = max(len(x.properties, len(y.properties)))
    union_properties = len(x.properties | y.properties)
    assert max_properties <= union_properties

    return union_properties - max_properties


def load_entities(entity_file):
    return [Entity.from_json(json.loads(line)) for line in entity_file]


def zero_distance_clusters():
    entities = load_entities(ENTITY_FILE)
    entities.sort(key=lambda e: e.complexity)
    print(f'loading {len(entities)} entities')

    clusters = [entities.pop().to_cluster()]
    for e in tqdm.tqdm(entities):
        for c in clusters:
            if entity_distance(e, c) == 0:
                c.add_entity(e)
                break
        else:
            clusters.append(e.to_cluster())

    clusters.sort(key=lambda c: len(c.entities))
    for c in clusters:
        print(f'cluster with complexity {c.complexity}, with {len(c.entities)} entities')
    print(f'found {len(clusters)} using distance 0')


if __name__ == "__main__":
    zero_distance_clusters()
