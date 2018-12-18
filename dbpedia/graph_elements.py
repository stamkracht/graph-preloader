import glob
import json
import multiprocessing
import os
import sys
from collections import Counter, defaultdict, UserDict

import requests
from bs4 import BeautifulSoup
from rdflib import Literal
from rdflib.plugins.parsers.ntriples import NTriplesParser
from requests import RequestException

from dbpedia.compute_parts import compute_parts
from dbpedia.utils import base_path

OWL_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs'
MULTIVALUED_URI_PROPS = {
    OWL_SAME_AS,
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    'http://dbpedia.org/ontology/wikiPageExternalLink',
}


def transform_part(
        input_path,
        global_id_marker,
        part_name,
        left,
        right,
        prefixer=None,
):
    print(f'starting {part_name}: {left} -- {right}')
    with open(input_path, 'rb') as in_file:
        in_file.seek(left)
        part_bytes = in_file.read(right - left)
        part_str = part_bytes.decode('utf8')  # wasteful
        with PropertyGraphSink(global_id_marker, part_name, prefixer) as sink:
            ntp = NTriplesParser(sink=sink)
            ntp.parsestring(part_str)

    triple_count = sum(sink.predicate_count.values())
    print(f'finished {part_name}: {triple_count} triples')
    return part_name, dict(sink.predicate_count)


def make_graph_elements(args):
    print(f'Reading from {args.input_path} ...')

    prefixer = None
    if args.shorten_uris:
        prefixer = NamespacePrefixer()

    if args.parallel:
        pool = multiprocessing.Pool()
        tasks = []

        for part_path, left, right in compute_parts(args):
            tasks.append(pool.apply_async(
                transform_part, (
                    args.input_path,
                    args.global_id_marker,
                    part_path,
                    left,
                    right,
                    prefixer,
                )
            ))

        results = [
            task.get(timeout=args.task_timeout)
            for task in tasks
        ]
        pool.close()
    else:
        results = [
            transform_part(
                args.input_path,
                args.global_id_marker,
                part_path,
                left,
                right,
                prefixer,
            )
            for part_path, left, right in compute_parts(args)
        ]

    pcounts_path = os.path.join(args.output_dir, 'predicate-counts.json')
    with open(pcounts_path, 'w') as pcounts_file:
        json.dump(dict(results), pcounts_file, indent=4)
    print(f'\nDone! Predicate counts have been saved to {pcounts_path}')


class PropertyGraphSink:

    def __init__(self, global_id_marker, part_name, prefixer=None):
        self.global_id_marker = global_id_marker
        self.part_name = part_name
        self.prefixer = prefixer
        self.predicate_count = Counter()
        self.vertex_buffer = defaultdict(list)
        self.edge_buffer = []
        self.last_subject = None

    def __enter__(self):
        if glob.glob(f'{self.part_name}*'):
            print(f'WARN: files for {self.part_name} already '
                  f'exist and will be appended to', file=sys.stderr)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(self.part_name, file=sys.stderr)
            print(self.vertex_buffer, file=sys.stderr)
            print(self.edge_buffer, file=sys.stderr)
        else:
            self.flush_buffers()

    def triple(self, subj, pred, obj):
        if self.global_id_marker not in subj:
            return

        qn_subj, qn_pred, qn_obj = str(subj), str(pred), str(obj)
        if self.prefixer:
            qn_subj = self.prefixer.qname(subj)
            qn_pred = self.prefixer.qname(pred)
            qn_obj = self.prefixer.qname(obj)

        self.predicate_count[qn_pred] += 1
        if subj != self.last_subject:
            self.flush_buffers()
            self.last_subject = subj

        if self.global_id_marker in obj:
            if subj == obj and str(pred) == OWL_SAME_AS:
                # ignore "dbg:A owl:sameAs dbg:A"
                pass
            else:
                # create an edge
                self.edge_buffer.append({
                    'outv': qn_subj,
                    'label': qn_pred,
                    'inv': qn_obj
                })
        else:
            # we'll add something to the vertex buffer
            self.vertex_buffer['id'] = qn_subj
            if isinstance(obj, Literal):
                if obj.language:
                    # literals with language tag become vertex props
                    vertex_prop = self.make_vertex_prop(
                        obj.toPython(),
                        obj.language
                    )
                    try:
                        self.vertex_buffer[qn_pred].append(vertex_prop)
                    except AttributeError:
                        self.vertex_buffer[qn_pred] = [
                            self.make_vertex_prop(self.vertex_buffer[qn_pred]),
                            vertex_prop
                        ]
                elif self.vertex_buffer[qn_pred]:
                    # there is an existing value for this predicate
                    try:
                        # plain literal becomes vertex prop
                        self.vertex_buffer[qn_pred].append(
                            self.make_vertex_prop(self.vertex_buffer[qn_pred])
                        )
                    except AttributeError:
                        print(f'WARN: discarding triple (multiple values, same predicate) -- '
                              f'{subj} {pred} {obj}', file=sys.stderr)
                else:
                    # plain or typed literal
                    if obj.datatype and 'dbpedia.org/datatype' in obj.datatype:
                        self.vertex_buffer[qn_pred] = obj.n3()
                    else:
                        self.vertex_buffer[qn_pred] = obj.toPython()

            elif str(pred) in MULTIVALUED_URI_PROPS:
                # append simple multivalued prop
                self.vertex_buffer[qn_pred].append(qn_obj)
            else:
                # convert external URI to prop
                self.vertex_buffer[qn_pred] = str(obj)

    def flush_buffers(self):
        self.flush_vertex()
        self.flush_edges()

    def flush_vertex(self):
        if self.vertex_buffer:
            with open(f'{self.part_name}_vertices.jsonl', 'a', encoding='utf8') as out_file:
                json.dump(self.vertex_buffer, out_file, default=str)
                out_file.write('\n')

        self.vertex_buffer = defaultdict(list)

    def flush_edges(self):
        if self.edge_buffer:
            with open(f'{self.part_name}_edges.jsonl', 'a', encoding='utf8') as out_file:
                for edge in self.edge_buffer:
                    json.dump(edge, out_file, default=str)
                    out_file.write('\n')

        self.edge_buffer = []

    @staticmethod
    def make_vertex_prop(value, language=None):
        return {
            'value': value,
            'language': language
        }


class NamespacePrefixer(UserDict):

    def __init__(self, mapping=None, **kwargs):
        super().__init__(mapping, **kwargs)
        self.default_namespaces_url = 'http://dbpedia.org/sparql?nsdecl'
        self.default_namespaces_file = base_path('default-namespaces.json')
        if not self.data:
            self.load_default_namespaces()

        # overrides
        self['https://global.dbpedia.org/id/'] = 'dbg'
        self['http://www.wikidata.org/entity/'] = 'wde'

    def qname(self, uri):
        try:
            namespace, local_name = self.split_uri(uri)
        except ValueError:
            return uri

        if namespace in self:
            return f'{self[namespace]}:{local_name}'
        else:
            return uri

    def split_uri(self, uri):
        if '#' in uri:
            split_uri = uri.split('#', maxsplit=1)
            return f'{split_uri[0]}#', split_uri[1]

        elif '/' in uri:
            split_uri = uri.split('/')
            local_parts = []
            while split_uri:
                *split_uri, local_part = split_uri
                local_parts.append(local_part)
                namespace = '/'.join(split_uri) + '/'
                if namespace in self:
                    local_name = '/'.join(reversed(local_parts))
                    return namespace, local_name

        raise ValueError(f"Can't split '{uri}'")

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
