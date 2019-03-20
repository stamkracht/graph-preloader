import glob
import json
import multiprocessing
import os
import sys
from collections import Counter, defaultdict

from rdflib import Literal
from tqdm import tqdm

from dbpedia.compute_parts import compute_parts
from dbpedia.parser import NTriplesParser
from dbpedia.uri_transformation import NamespacePrefixer, SameThingClient

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
        samething_service=None,
        update_progress=None,
):
    print(f'starting {part_name}: {left} -- {right}')
    with open(input_path, 'rb') as in_file:
        with PropertyGraphSink(global_id_marker, part_name, prefixer, samething_service) as sink:
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
                    args.samething_service,
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
                args.samething_service,
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

    def __init__(self, global_id_marker, part_name, prefixer=None, samething_service=None):
        self.global_id_marker = global_id_marker
        self.part_name = part_name
        self.prefixer = prefixer
        self.predicate_count = Counter()
        self.vertex_buffer = defaultdict(list)
        self.edge_buffer = []
        self.last_subject = None

        self.samething_client = None
        if samething_service:
            self.samething_client = SameThingClient(samething_service)

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
            if not (subj == obj and str(pred) == OWL_SAME_AS):
                # append simple multivalued prop
                self.vertex_buffer[qn_pred].append(qn_obj)

        elif self.global_id_marker in obj:
            # create an edge
            wd_subj, wd_obj = None, None
            if self.samething_client:
                wd_subj = self.samething_client.fetch_wikidata_uri(subj)
                wd_obj = self.samething_client.fetch_wikidata_uri(obj)
                if self.prefixer:
                    wd_subj = self.prefixer.qname(wd_subj)
                    wd_obj = self.prefixer.qname(wd_obj)

            self.edge_buffer.append({
                'outv': wd_subj or qn_subj,
                'label': qn_pred,
                'inv': wd_obj or qn_obj
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
            if self.samething_client:
                wd_subj = self.samething_client.fetch_wikidata_uri(self.last_subject)
                if self.prefixer:
                    wd_subj = self.prefixer.qname(wd_subj)
                self.vertex_buffer['dbg:cluster-id'] = self.vertex_buffer['id']
                self.vertex_buffer['id'] = wd_subj

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
