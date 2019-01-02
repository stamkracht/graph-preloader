import argparse
import os
import sys
import time

from dbpedia.compute_parts import SEARCH_TYPE_CHOICES, BINARY_SEARCH_TYPE
from dbpedia.graph_elements import make_graph_elements
from dbpedia.utils import base_path

arg_parser = argparse.ArgumentParser(
    description='Transform sorted Databus NTriples into property graph-friendly JSON.',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)


def parse_arguments(arg_list, parser=arg_parser, **kwargs):
    args = parser.parse_args(arg_list)
    for arg_name, value in kwargs.items():
        setattr(args, arg_name, value)

    # apply computed defaults
    args.parts_file = getattr(
        args, 'parts_file', os.path.join(args.output_dir, 'parts.tsv')
    )
    args.backpedal_size = getattr(args, 'backpedal_size', args.jump_size // 10)
    return args


def cast_int(str_or_number):
    return int(float(str_or_number))


def get_timed_output_path(prefix='output'):
    this_second_hex = hex(int(time.time()))
    return base_path(f'{prefix}_{this_second_hex[2:]}/')


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
    default=os.environ.get('OUTPUT_DIR', get_timed_output_path()),
    help='the JSON output directory path'
)
arg_parser.add_argument(
    '--parallel',
    action='store_true',
    help='transform parts in parallel using a multiprocessing pool'
)
arg_parser.add_argument(
    '--shorten-uris',
    action='store_true',
    help='shorten URIs by replacing known namespaces with their corresponding prefix'
)
arg_parser.add_argument(
    '--target-size',
    type=cast_int,
    default=os.environ.get('TARGET_SIZE', '500e6'),  # bytes
    help='the approximate size of parts in bytes'
)
arg_parser.add_argument(
    '--global-id-marker',
    default=os.environ.get('GLOBAL_ID_MARKER', 'global.dbpedia.org/id/'),
    help='only triples with this marker in the subject will be transformed'
)
arg_parser.add_argument(
    '--id-marker-prefix',
    type=lambda x: bytes(x, 'ascii'),
    default=os.environ.get('ID_MARKER_PREFIX', '<https://'),
    help='the characters that precede the `global_id_marker` in each triple'
)
arg_parser.add_argument(
    '--parts-file',
    default=os.environ.get('PARTS_FILE', argparse.SUPPRESS),
    help='the file in which output files are listed with '
         'corresponding input file positions (left and right) '
         '(default: <output_dir>/parts.tsv)'
)
arg_parser.add_argument(
    '--task-timeout',
    type=int,
    default=os.environ.get('TASK_TIMEOUT', 10 * 60),  # seconds
    help='the number of seconds a "transform part" task is allowed to run '
         '(applies only to parallel execution)'
)
arg_parser.add_argument(
    '--search-type',
    choices=SEARCH_TYPE_CHOICES,
    default=os.environ.get('SEARCH_TYPE', BINARY_SEARCH_TYPE),
    help='the type of search to use to skip to the first `global_id_marker` triple'
)
arg_parser.add_argument(
    '--bin-search-limit',
    type=int,
    default=os.environ.get('BIN_SEARCH_LIMIT', 120),
    help='the maximum number of iterations of the binary search main loop'
)
arg_parser.add_argument(
    '--jump-size',
    type=cast_int,
    default=os.environ.get('JUMP_SIZE', '350e6'),
    help='the size of forward jumps in bytes'
)
arg_parser.add_argument(
    '--backpedal-size',
    type=cast_int,
    default=os.environ.get('BACKPEDAL_SIZE', argparse.SUPPRESS),
    help='the size of backpedals in bytes (default: <jump_size> // 10)'
)


if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    try:
        make_graph_elements(args)
    except FileNotFoundError as err:
        print(err, file=sys.stderr)
        arg_parser.print_help(sys.stderr)
        exit(1)
