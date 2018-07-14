import csv
import glob
import json
import multiprocessing
import os
import sys
from collections import Counter, defaultdict

from rdflib import Literal
from rdflib.plugins.parsers.ntriples import NTriplesParser

IN_PATH = 'split-test.nt'
OUT_BASE = './out'
PARTS_FILE = 'parts.tsv'
TARGET_SIZE = 3 * 1024
TASK_TIMEOUT = 10 * 60
GLOBAL_ID_MARKER = 'id.dbpedia.org/global/'
JUMP_SIZE = 15000 #350e6
BACKPEDAL_SIZE = JUMP_SIZE // 10

OWL_SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs'
MULTIVALUED_URI_PROPS = {
    OWL_SAME_AS,
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    'http://dbpedia.org/ontology/wikiPageExternalLink',
}


def transform_part(part_name, begin, end):
    with open(IN_PATH, 'rb') as in_file:
        in_file.seek(begin)
        part_bytes = in_file.read(end - begin)
        part_str = part_bytes.decode('utf8')  # wasteful
        with PropertyGraphSink(part_name) as sink:
            ntp = NTriplesParser(sink=sink)
            ntp.parsestring(part_str)

    return part_name, dict(sink.predicate_count)


def compute_parts(target_size=TARGET_SIZE):

    with open(IN_PATH, 'rb') as in_file:
        file_end = in_file.seek(0, 2)

        # jump and backpedal to find the first global URI
        chunk_end = seek_first_global_subject(in_file, file_end)

        with open(os.path.join(OUT_BASE, PARTS_FILE), 'w') as parts_file:
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

                part_name = os.path.join(OUT_BASE, f'part-{part_number:03}')
                tsv_writer.writerow([part_name, chunk_start, chunk_end])
                yield part_name, chunk_start, chunk_end


def read_subject_from_line(file_obj):
    return file_obj.readline().split(b'> <')[0]


def seek_first_global_subject(file_obj, file_end):
    cursor = 0
    subj_str = b''
    id_marker = GLOBAL_ID_MARKER.encode('utf8')

    print('Looking for the first line with a global URI as subject...')

    # JUMP
    while id_marker not in subj_str and cursor < file_end:
        cursor, subj_str = seek_subject_at(file_obj, cursor, +JUMP_SIZE)
        print('jump', cursor, subj_str)

    # BACKPEDAL
    while id_marker in subj_str and cursor > 0:
        cursor, subj_str = seek_subject_at(file_obj, cursor, -BACKPEDAL_SIZE)
        print('backpedal', cursor, subj_str)

    if 0 < cursor < file_end:
        # STEP
        while id_marker not in subj_str and cursor < file_end:
            cursor = file_obj.tell()
            subj_str = read_subject_from_line(file_obj)
            print('step', cursor, subj_str)

        if id_marker not in subj_str:
            print('WARN: did not find first global URI', file=sys.stderr)
            cursor = 0
    else:
        print('WARN: did not find first global URI', file=sys.stderr)
        cursor = 0

    file_obj.seek(cursor)
    return file_obj.tell()


def seek_subject_at(file_obj, cursor, delta):
    file_obj.seek(cursor + delta)
    file_obj.readline()
    new_cursor = file_obj.tell()
    if new_cursor == cursor:
        raise ValueError(
            f'The cursor is not moving at byte {cursor}.\n'
            'Increase JUMP_SIZE or BACKPEDAL_SIZE for this input file.'
        )

    subj_str = read_subject_from_line(file_obj)
    return new_cursor, subj_str


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
        if GLOBAL_ID_MARKER not in subj:
            return

        self.predicate_count[pred] += 1
        if subj != self.last_subject:
            self.flush_buffers()
            self.last_subject = subj

        if GLOBAL_ID_MARKER in obj:
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


def main(parallel=True):
    if parallel:
        pool = multiprocessing.Pool()
        tasks = []

        for part_path, begin, end in compute_parts():
            tasks.append(pool.apply_async(
                transform_part, (part_path, begin, end))
            )

        results = [
            task.get(timeout=TASK_TIMEOUT)
            for task in tasks
        ]
        pool.close()
    else:
        results = [
            transform_part(part_path, begin, end)
            for part_path, begin, end in compute_parts()
        ]

    for res in results:
        print(res)


if __name__ == "__main__":
    if not os.path.exists(OUT_BASE):
        os.makedirs(OUT_BASE)

    main(parallel=False)
