import functools
import json
import re
from collections import UserDict

import requests
from bs4 import BeautifulSoup
from requests import RequestException
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from dbpedia.utils import base_path, get_verbosity


class NamespacePrefixer(UserDict):

    def __init__(self, mapping=None, **kwargs):
        super().__init__(mapping, **kwargs)
        self.default_namespaces_url = 'http://dbpedia.org/sparql?nsdecl'
        self.default_namespaces_file = base_path('default-namespaces.json')
        if not self.data:
            self.load_default_namespaces()

        self.separators = '/#:'
        self.separator_re = re.compile(f'([{self.separators}])')

        # overrides
        self['https://global.dbpedia.org/id/'] = 'dbg'
        self['http://www.wikidata.org/entity/'] = 'wde'

        # reverse mapping
        self.reverse_dict = {pf: ns for ns, pf in self.items()}

    def qname(self, iri):
        try:
            namespace, local_name = self.split_iri(iri)
        except ValueError:
            return iri

        if namespace in self:
            return f'{self[namespace]}:{local_name}'
        else:
            return iri

    def reverse(self, qname):
        try:
            prefix, local_name = qname.split(':', maxsplit=1)
        except ValueError:
            return qname

        if prefix in self.reverse_dict:
            namespace = self.reverse_dict[prefix]
            separator = '#' if namespace.endswith('.owl') else ''
            return f'{namespace}{separator}{local_name}'
        else:
            return qname

    def split_iri(self, iri):
        iri_split = self.separator_re.split(iri)

        local_parts = []
        while iri_split:
            *iri_split, local_part = iri_split
            local_parts.append(local_part)
            namespace = ''.join(iri_split)

            if namespace in self:
                local_name = ''.join(reversed(local_parts))
                if local_name and local_name[0] in self.separators:
                    local_name = local_name[1:]

                return namespace, local_name

        raise ValueError(f"Can't split '{iri}'")

    def load_default_namespaces(self):
        try:
            ns_mapping = self.fetch_default_namespaces()
        except (AttributeError, ConnectionError, RequestException):
            print(f"Couldn't update namespaces from {self.default_namespaces_url}")
            with open(self.default_namespaces_file) as ns_file:
                ns_mapping = json.load(ns_file)

        self.update(ns_mapping)

    def fetch_default_namespaces(self):
        print(f'Downloading namespaces from {self.default_namespaces_url} ...')
        nsdecl_resp = requests.get(self.default_namespaces_url)
        nsdecl_soup = BeautifulSoup(nsdecl_resp.text, 'lxml')
        ns_table = nsdecl_soup.find('table', class_='tableresult')

        ns_to_prefix = {}
        for tr in ns_table.find_all('tr'):
            prefix_td = tr.find('td')
            namespace_a = tr.find('a')
            if prefix_td and namespace_a:
                ns_to_prefix[namespace_a['href']] = prefix_td.text

        with open(self.default_namespaces_file, 'w') as ns_file:
            json.dump(ns_to_prefix, ns_file, indent=4)

        return ns_to_prefix


class SameThingClient:
    wikidata_base = f'http://www.wikidata.org/entity/'

    def __init__(self, samething_service_url):
        self.samething_service_url = samething_service_url
        self.session = requests.Session()

        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    @functools.lru_cache(maxsize=4096)
    def fetch_wikidata_uri(self, resource_iri):
        canonical_iri = None
        request_uri = f'{self.samething_service_url}lookup/?meta=off&uri={resource_iri}'
        response = self.session.get(request_uri)
        if response.ok:
            for iri in response.json()['locals']:
                if iri.startswith(self.wikidata_base):
                    canonical_iri = iri
                    break

        if not canonical_iri:
            canonical_iri = resource_iri
            if get_verbosity() > 1:
                print(f'same-thing: no Wikidata URI found by {request_uri}')

        return canonical_iri
