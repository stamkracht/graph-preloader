import glob
import json
import multiprocessing
import sys
from collections import Counter, defaultdict

from rdflib import Literal
from rdflib.plugins.parsers.ntriples import NTriplesParser

from dbpedia.compute_parts import compute_parts


OWL_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs'
MULTIVALUED_URI_PROPS = {
    OWL_SAME_AS,
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    'http://dbpedia.org/ontology/wikiPageExternalLink',
}


def transform_part(input_path, global_id_marker, part_name, left, right):
    print(f'starting {part_name}: {left} -- {right}')
    with open(input_path, 'rb') as in_file:
        in_file.seek(left)
        part_bytes = in_file.read(right - left)
        part_str = part_bytes.decode('utf8')  # wasteful
        with PropertyGraphSink(global_id_marker, part_name) as sink:
            ntp = NTriplesParser(sink=sink)
            ntp.parsestring(part_str)

    triple_count = sum(sink.predicate_count.values())
    print(f'finished {part_name}: {triple_count} triples')
    return part_name, dict(sink.predicate_count)


def make_graph_elements(args):
    print(f'Reading from {args.input_path} ...')

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
                    right
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
                right
            )
            for part_path, left, right in compute_parts(args)
        ]

    for res in results:
        print(res)


class PropertyGraphSink(object):
    def __init__(self, global_id_marker, part_name):
        self.global_id_marker = global_id_marker
        self.part_name = part_name
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

        self.predicate_count[pred.n3()] += 1
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
                    'outv': subj,
                    'label': pred,
                    'inv': obj
                })
        else:
            # we'll add something to the vertex buffer
            self.vertex_buffer['id'] = subj
            if isinstance(obj, Literal):
                if obj.language:
                    # literals with language tag become vertex props
                    vertex_prop = self.make_vertex_prop(
                        obj.toPython(),
                        obj.language
                    )
                    try:
                        self.vertex_buffer[pred].append(vertex_prop)
                    except AttributeError:
                        self.vertex_buffer[pred] = [
                            self.make_vertex_prop(self.vertex_buffer[pred]),
                            vertex_prop
                        ]
                elif self.vertex_buffer[pred]:
                    # plain literal becomes vertex prop
                    self.vertex_buffer[pred].append(
                        self.make_vertex_prop(self.vertex_buffer[pred])
                    )
                else:
                    # plain or typed literal
                    if obj.datatype and 'dbpedia.org/datatype' in obj.datatype:
                        self.vertex_buffer[pred] = obj.n3()
                    else:
                        self.vertex_buffer[pred] = obj.toPython()

            elif str(pred) in MULTIVALUED_URI_PROPS:
                # append simple multivalued prop
                self.vertex_buffer[pred].append(obj.toPython())
            else:
                # convert external URI to prop
                self.vertex_buffer[pred] = obj.toPython()

    def flush_buffers(self):
        self.flush_vertex()
        self.flush_edges()

    def flush_vertex(self):
        with open(f'{self.part_name}_vertices.jsonl', 'a', encoding='utf8') as out_file:
            if self.vertex_buffer:
                json.dump(self.vertex_buffer, out_file, default=str)
                out_file.write('\n')

        self.vertex_buffer = defaultdict(list)

    def flush_edges(self):
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
