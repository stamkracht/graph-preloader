import argparse
import csv
import glob
import json
import multiprocessing
import os
import sys
from collections import Counter, defaultdict

from rdflib import Literal
from rdflib.plugins.parsers.ntriples import NTriplesParser


parser = argparse.ArgumentParser(
    description='Transform sorted Databus NTriples into property graph-friendly JSON.',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)


def cast_int(str_or_number):
    return int(float(str_or_number))


parser.add_argument(
    'input_path',
    nargs='?',
    type=os.path.abspath,
    default=os.environ.get('INPUT_PATH', os.path.abspath('sorted.nt')),
    help='the Databus NTriples input file path'
)
parser.add_argument(
    'output_dir',
    nargs='?',
    type=os.path.abspath,
    default=os.environ.get('OUTPUT_DIR', os.path.abspath('output')),
    help='the JSON output directory path'
)
parser.add_argument(
    '--parallel',
    action='store_true',
    help='transform parts in parallel using a multiprocessing pool'
)
# TODO: choose reasonable default (500e6)
parser.add_argument(
    '--target_size',
    type=cast_int,
    default=os.environ.get('TARGET_SIZE', '30e3'),  # bytes
    help='the approximate size of parts in bytes'
)
parser.add_argument(
    '--global_id_marker',
    default=os.environ.get('GLOBAL_ID_MARKER', 'id.dbpedia.org/global/'),
    help='only triples with this marker in the subject will be transformed'
)
parser.add_argument(
    '--id_marker_prefix',
    type=lambda x: bytes(x, 'ascii'),
    default=os.environ.get('ID_MARKER_PREFIX', '<http://'),
    help='the characters that precede the `global_id_marker` in each triple'
)
parser.add_argument(
    '--parts_file',
    default=os.environ.get('PARTS_FILE', argparse.SUPPRESS),
    help='the file in which output files are listed with '
         'corresponding input file positions (left and right) '
         '(default: <output_dir>/parts.tsv)'
)
parser.add_argument(
    '--task_timeout',
    type=int,
    default=os.environ.get('TASK_TIMEOUT', 10 * 60),  # seconds
    help='the number of seconds a "transform part" task is allowed to run '
         '(applies only to parallel execution)'
)
BINARY_SEARCH_TYPE, JUMP_SEARCH_TYPE = 'binary', 'jump'
SEARCH_TYPE_CHOICES = [BINARY_SEARCH_TYPE, JUMP_SEARCH_TYPE]
parser.add_argument(
    '--search_type',
    choices=SEARCH_TYPE_CHOICES,
    default=os.environ.get('SEARCH_TYPE', BINARY_SEARCH_TYPE),
    help='the type of search to use to skip to the first `global_id_marker` triple'
)
parser.add_argument(
    '--bin_search_limit',
    type=int,
    default=os.environ.get('BIN_SEARCH_LIMIT', 120),
    help='the maximum number of iterations of the binary search main loop'
)
# TODO: choose reasonable default (350e6)
parser.add_argument(
    '--jump_size',
    type=cast_int,
    default=os.environ.get('JUMP_SIZE', '15e3'),
    help='the size of forward jumps in bytes'
)
parser.add_argument(
    '--backpedal_size',
    type=cast_int,
    default=os.environ.get('BACKPEDAL_SIZE', argparse.SUPPRESS),
    help='the size of backpedals in bytes (default: <jump_size> // 10'
)

args = parser.parse_args()
args.parts_file = getattr(args, 'parts_file', os.path.join(args.output_dir, 'parts.tsv'))
args.backpedal_size = getattr(args, 'backpedal_size', args.jump_size // 10)


OWL_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs'
MULTIVALUED_URI_PROPS = {
    OWL_SAME_AS,
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    'http://dbpedia.org/ontology/wikiPageExternalLink',
}


def transform_part(part_name, left, right):
    print(f'starting {part_name}: {left} -- {right}')
    with open(args.input_path, 'rb') as in_file:
        in_file.seek(left)
        part_bytes = in_file.read(right - left)
        part_str = part_bytes.decode('utf8')  # wasteful
        with PropertyGraphSink(part_name) as sink:
            ntp = NTriplesParser(sink=sink)
            ntp.parsestring(part_str)

    triple_count = sum(sink.predicate_count.values())
    print(f'finished {part_name}: {triple_count} triples')
    return part_name, dict(sink.predicate_count)


def compute_parts(target_size=args.target_size):

    with open(args.input_path, 'rb') as in_file:
        file_end = in_file.seek(0, os.SEEK_END)

        # hop to the line with the first global URI subject
        chunk_end = seek_first_global_subject(in_file, file_end)

        with open(args.parts_file, 'w') as parts_file:
            tsv_writer = csv.writer(parts_file, delimiter='\t')
            part_number = 0

            while chunk_end < file_end:
                part_number += 1
                chunk_start = chunk_end

                # seek to the first line break after target
                in_file.seek(chunk_start + target_size)
                in_file.readline()

                # find the transition between two subjects
                final_subject = read_subject_from_line(in_file)
                bookmark = in_file.tell()
                while True:
                    new_subject = read_subject_from_line(in_file)
                    if new_subject and new_subject == final_subject:
                        bookmark = in_file.tell()
                    else:
                        # seek to the end of the line with a `final_subject`
                        in_file.seek(bookmark)
                        chunk_end = bookmark
                        break

                part_name = os.path.join(args.output_dir, f'part-{part_number:03}')
                tsv_writer.writerow([part_name, chunk_start, chunk_end])
                yield part_name, chunk_start, chunk_end


def read_subject_from_line(file_obj):
    return file_obj.readline().split(b'> <')[0]


def seek_first_global_subject(file_obj, file_end, search_type=args.search_type):
    id_marker = args.global_id_marker.encode('utf8')

    print('Looking for the first line with a global URI as subject...')
    if search_type == BINARY_SEARCH_TYPE:
        cursor = binary_search(file_obj, id_marker, file_end)
    else:
        cursor = jump_backpedal_and_step(file_obj, id_marker, file_end)

    return file_obj.seek(cursor)


def binary_search(file_obj, id_marker, file_end):
    left = cursor = 0
    right = file_end
    id_subj_str = args.id_marker_prefix + id_marker

    for attempt in range(args.bin_search_limit):
        try:
            cursor, subj_str = seek_subject_at(
                file_obj,
                left,
                +(right - left) // 2
            )
        except StopIteration:
            cursor = step_to_marked_line(file_obj, left, id_marker, right)
            break

        if subj_str < id_subj_str:
            left = cursor
            print('forw', left, right, subj_str)
        else:
            right = cursor
            print('back', left, right, subj_str)

    return cursor


def jump_backpedal_and_step(file_obj, id_marker, file_end):
    subj_str = b''
    cursor = 0

    # JUMP
    while id_marker not in subj_str and cursor < file_end:
        cursor, subj_str = seek_subject_at(file_obj, cursor, +args.jump_size)
        print('jump', cursor, subj_str)

    # BACKPEDAL
    while id_marker in subj_str and cursor > 0:
        cursor, subj_str = seek_subject_at(file_obj, cursor, -args.backpedal_size)
        print('backpedal', cursor, subj_str)

    if 0 < cursor < file_end:
        # STEP
        cursor = step_to_marked_line(file_obj, cursor, id_marker, file_end)
    else:
        print('WARN: did not find first global URI', file=sys.stderr)
        cursor = 0

    return cursor


def seek_subject_at(file_obj, cursor, delta):
    file_obj.seek(cursor + delta)
    discard = file_obj.readline()
    if len(discard) >= abs(delta):
        raise StopIteration('Target lies between `cursor` and `new_cursor`')

    new_cursor = file_obj.tell()
    if new_cursor == cursor:
        raise ValueError(
            f'The cursor is not moving at byte {cursor}.\n'
            'Increase `jump_size` for this input file.'
        )

    subj_str = read_subject_from_line(file_obj)
    return new_cursor, subj_str


def step_to_marked_line(file_obj, cursor, id_marker, upper_limit):
    subj_str = b''
    file_obj.seek(cursor)
    while id_marker not in subj_str and cursor < upper_limit:
        cursor = file_obj.tell()
        subj_str = read_subject_from_line(file_obj)
        print('step', cursor, subj_str)

    if id_marker not in subj_str:
        print('WARN: did not find first global URI', file=sys.stderr)
        cursor = 0

    return file_obj.seek(cursor)


class PropertyGraphSink(object):
    def __init__(self, part_name):
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
        if args.global_id_marker not in subj:
            return

        self.predicate_count[pred.n3()] += 1
        if subj != self.last_subject:
            self.flush_buffers()
            self.last_subject = subj

        if args.global_id_marker in obj:
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


def make_graph_elements(parallel=args.parallel):
    print(f'Reading from {args.input_path} ...')

    if parallel:
        pool = multiprocessing.Pool()
        tasks = []

        for part_path, left, right in compute_parts():
            tasks.append(pool.apply_async(
                transform_part, (part_path, left, right))
            )

        results = [
            task.get(timeout=args.task_timeout)
            for task in tasks
        ]
        pool.close()
    else:
        results = [
            transform_part(part_path, left, right)
            for part_path, left, right in compute_parts()
        ]

    for res in results:
        print(res)


if __name__ == "__main__":
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    make_graph_elements()
