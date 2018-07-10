#! /usr/bin/env python3

"""separate properties and items from pull-data output

Takes the output from the pull-data script and sorts it into separate files for
items (Q) and properties (P)
"""

import json
import sys


def main():
    item_file = open('data/items.dump', 'w')
    property_file = open('data/properties.dump', 'w')

    for line in sys.stdin:
        entity = json.loads(line)

        ent_file = item_file if entity['id'][0] == 'Q' else property_file
        ent_file.write(line)


if __name__ == "__main__":
    main()
