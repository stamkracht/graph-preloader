import argparse
import os
import sys

from dbpedia.compute_parts import SEARCH_TYPE_CHOICES, BINARY_SEARCH_TYPE
from dbpedia.graph_elements import make_graph_elements


arg_parser = argparse.ArgumentParser(
    description='Transform sorted Databus NTriples into property graph-friendly JSON.',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)


def parse_arguments(arg_list, parser=arg_parser, **kwargs):
    args = parser.parse_args(arg_list)
    for arg_name, value in kwargs.items():
        setattr(args, arg_name, value)

    # apply computed defaults
    args.parts_file = getattr(args, 'parts_file',
                              os.path.join(args.output_dir, 'parts.tsv'))
    args.backpedal_size = getattr(args, 'backpedal_size', args.jump_size // 10)
    return args


def base_path(name_or_path):
    base_dir = os.path.dirname(
        os.path.realpath(__file__)
    )
    return os.path.join(base_dir, name_or_path)


def cast_int(str_or_number):
    return int(float(str_or_number))


arg_parser.add_argument(
    'input_path',
    nargs='?',
    type=os.path.abspath,
    default=os.environ.get('INPUT_PATH', base_path('sorted.nt')),
    help='the Databus NTriples input file path'
)
arg_parser.add_argument(
    'output_dir',
    nargs='?',
    type=os.path.abspath,
    default=os.environ.get('OUTPUT_DIR', base_path('output/')),
    help='the JSON output directory path'
)
arg_parser.add_argument(
    '--parallel',
    action='store_true',
    help='transform parts in parallel using a multiprocessing pool'
)
# TODO: choose reasonable default (500e6)
arg_parser.add_argument(
    '--target_size',
    type=cast_int,
    default=os.environ.get('TARGET_SIZE', '30e3'),  # bytes
    help='the approximate size of parts in bytes'
)
arg_parser.add_argument(
    '--global_id_marker',
    default=os.environ.get('GLOBAL_ID_MARKER', 'id.dbpedia.org/global/'),
    help='only triples with this marker in the subject will be transformed'
)
arg_parser.add_argument(
    '--id_marker_prefix',
    type=lambda x: bytes(x, 'ascii'),
    default=os.environ.get('ID_MARKER_PREFIX', '<http://'),
    help='the characters that precede the `global_id_marker` in each triple'
)
arg_parser.add_argument(
    '--parts_file',
    default=os.environ.get('PARTS_FILE', argparse.SUPPRESS),
    help='the file in which output files are listed with '
         'corresponding input file positions (left and right) '
         '(default: <output_dir>/parts.tsv)'
)
arg_parser.add_argument(
    '--task_timeout',
    type=int,
    default=os.environ.get('TASK_TIMEOUT', 10 * 60),  # seconds
    help='the number of seconds a "transform part" task is allowed to run '
         '(applies only to parallel execution)'
)
arg_parser.add_argument(
    '--search_type',
    choices=SEARCH_TYPE_CHOICES,
    default=os.environ.get('SEARCH_TYPE', BINARY_SEARCH_TYPE),
    help='the type of search to use to skip to the first `global_id_marker` triple'
)
arg_parser.add_argument(
    '--bin_search_limit',
    type=int,
    default=os.environ.get('BIN_SEARCH_LIMIT', 120),
    help='the maximum number of iterations of the binary search main loop'
)
# TODO: choose reasonable default (350e6)
arg_parser.add_argument(
    '--jump_size',
    type=cast_int,
    default=os.environ.get('JUMP_SIZE', '15e3'),
    help='the size of forward jumps in bytes'
)
arg_parser.add_argument(
    '--backpedal_size',
    type=cast_int,
    default=os.environ.get('BACKPEDAL_SIZE', argparse.SUPPRESS),
    help='the size of backpedals in bytes (default: <jump_size> // 10'
)


if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    make_graph_elements(args)
