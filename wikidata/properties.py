#! /usr/bin/env python3

"""supporting functions to transform property values"""

import json

PROPERTY_DATABASE = {}
PROPERTIES_FILE = 'data/properties.dump'


def get_key(property_id):
    """get property label from id"""
    return PROPERTY_DATABASE[property_id]


def get_value(datavalue):
    """get serializable property value from datavalue"""
    t, v = datavalue['type'], datavalue['value']
    if t == 'string':
        return v
    elif t in ('amount', 'quantity'):
        return float(v['amount'])
    elif t == 'time':
        return v['time']
    elif t == 'globecoordinate':
        return {'latitude': v['latitude'], 'longitude': v['longitude']}


def init_database():
    for line in open(PROPERTIES_FILE):
        p = json.loads(line)
        PROPERTY_DATABASE[p['id']] = p['labels']['en']['value']


init_database()
