import functools
import glob
import json
import multiprocessing
import os
import re
import sys
from collections import Counter, defaultdict, UserDict

import requests
from bs4 import BeautifulSoup
from rdflib import Literal
from requests import RequestException
from tqdm import tqdm

from dbpedia.compute_parts import compute_parts
from dbpedia.parser import NTriplesParser
from dbpedia.utils import base_path

OWL_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs'
MULTIVALUED_URI_PROPS = {
    OWL_SAME_AS,
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    'http://dbpedia.org/ontology/wikiPageExternalLink',
}
SAMETHING_SERVICE = 'https://e.hum.uva.nl/same-thing/' #'http://downloads.dbpedia.org/same-thing/'


def transform_part(
        input_path,
        global_id_marker,
        part_name,
        left,
        right,
        prefixer=None,
        update_progress=None,
):
    print(f'starting {part_name}: {left} -- {right}')
    with open(input_path, 'rb') as in_file:
        with PropertyGraphSink(global_id_marker, part_name, prefixer) as sink:
            ntp = NTriplesParser(sink=sink, update_progress=update_progress)
            ntp.parse(in_file, left=left, right=right)

    triple_count = sum(sink.predicate_count.values())
    print(f'finished {part_name}: {triple_count} triples')
    return part_name, dict(sink.predicate_count)


def make_graph_elements(args):
    print(f'Reading from {args.input_path} ...')

    prefixer = None
    parts = compute_parts(args)
    if args.shorten_uris:
        prefixer = NamespacePrefixer()

    progress_bar = tqdm(
        total=os.path.getsize(args.input_path),
        unit='b',
        unit_scale=True,
        unit_divisor=1024,
        mininterval=0.4,
    )

    if args.parallel:
        pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
        multi_manager = multiprocessing.Manager()
        queue = multi_manager.Queue(1024)
        tasks = []

        def progress_listener(queue):
            for item in iter(queue.get, None):
                progress_bar.update(item)

        listener_process = multiprocessing.Process(target=progress_listener, args=(queue,))
        listener_process.start()

        for part_path, left, right in parts:
            tasks.append(pool.apply_async(
                transform_part, (
                    args.input_path,
                    args.global_id_marker,
                    part_path,
                    left,
                    right,
                    prefixer,
                    queue.put
                )
            ))

        meta_results = [
            task.get(timeout=args.task_timeout)
            for task in tasks
        ]
        pool.close()

        queue.put(None)
        listener_process.join()
    else:
        meta_results = [
            transform_part(
                args.input_path,
                args.global_id_marker,
                part_path,
                left,
                right,
                prefixer,
                progress_bar.update
            )
            for part_path, left, right in parts
        ]

    pcounts_path = os.path.join(args.output_dir, 'predicate-counts.json')
    with open(pcounts_path, 'w') as pcounts_file:
        json.dump(dict(meta_results), pcounts_file, indent=4)

    progress_bar.close()
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

        self.vertex_buffer['id'] = qn_subj

        if str(pred) in MULTIVALUED_URI_PROPS:
            # ignore "dbg:A owl:sameAs dbg:A"
            if not subj == obj and str(pred) == OWL_SAME_AS:
                # append simple multivalued prop
                self.vertex_buffer[qn_pred].append(qn_obj)

        elif self.global_id_marker in obj:
            # create an edge
            wd_obj = fetch_wikidata_uri(obj)
            self.edge_buffer.append({
                'outv': qn_subj,
                'label': qn_pred,
                'inv': qn_obj
            })
        # we'll add something to the vertex buffer
        elif isinstance(obj, Literal):
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
            else:
                # plain or typed literal
                if obj.datatype and 'dbpedia.org/datatype' in obj.datatype:
                    native_obj = obj.n3()
                else:
                    native_obj = obj.toPython()

                if self.vertex_buffer[qn_pred]:
                    # there is an existing value for this predicate
                    if hasattr(self.vertex_buffer[qn_pred], 'append'):
                        if isinstance(self.vertex_buffer[qn_pred][0], dict):
                            # plain literal becomes vertex prop
                            native_obj = self.make_vertex_prop(native_obj)

                        self.vertex_buffer[qn_pred].append(native_obj)
                    else:
                        # existing and new value are combined in a list
                        self.vertex_buffer[qn_pred] = [
                            self.vertex_buffer[qn_pred],
                            native_obj
                        ]
                else:
                    self.vertex_buffer[qn_pred] = native_obj

        else:
            # convert external URI to prop
            self.vertex_buffer[qn_pred] = str(obj)

    def flush_buffers(self):
        self.flush_vertex()
        self.flush_edges()

    def flush_vertex(self):
        if self.vertex_buffer:
            # todo: if self.samething_service: ...
            wd_subj = fetch_wikidata_uri(self.last_subject)
            if self.prefixer:
                wd_subj = self.prefixer.qname(wd_subj)
            self.vertex_buffer['dbg:cluster-id'] = self.last_subject
            self.vertex_buffer['id'] = wd_subj

            with open(f'{self.part_name}_vertices.jsonl', 'a', encoding='utf8') as out_file:
                json.dump(self.vertex_buffer, out_file, default=str)
                out_file.write('\n')

        self.vertex_buffer = defaultdict(list)

    def flush_edges(self):
        if self.edge_buffer:
            with open(f'{self.part_name}_edges.jsonl', 'a', encoding='utf8') as out_file:
                for edge in self.edge_buffer:
                    # todo: if self.samething_service: ...
                    wd_subj = fetch_wikidata_uri(self.last_subject)
                    if self.prefixer:
                        wd_subj = self.prefixer.qname(wd_subj)
                        wd_obj = self.prefixer.qname(
                            fetch_wikidata_uri(self.prefixer.reverse(edge['inv']))
                        )
                    else:
                        wd_obj = fetch_wikidata_uri(edge['inv'])

                    edge['outv'] = wd_subj
                    edge['inv'] = wd_obj

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
            return f'{self.reverse_dict[prefix]}{local_name}'
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


@functools.lru_cache(maxsize=4096)
def fetch_wikidata_uri(resource_iri):
    wikidata_root = f'http://www.wikidata.org/entity/'
    canonical_iri = None
    response = requests.get(f'{SAMETHING_SERVICE}lookup/?meta=off&uri={resource_iri}')
    if response.ok:
        for iri in response.json()['locals']:
            if iri.startswith(wikidata_root):
                canonical_iri = iri
                break

    if not canonical_iri:
        canonical_iri = resource_iri
        print(f'same-thing: item id {resource_iri} was not found. Not transforming')

    return canonical_iri
