import csv
import os
import sys

BINARY_SEARCH_TYPE, JUMP_SEARCH_TYPE = 'binary', 'jump'
SEARCH_TYPE_CHOICES = [BINARY_SEARCH_TYPE, JUMP_SEARCH_TYPE]


def compute_parts(args):

    with open(args.input_path, 'rb') as in_file:
        file_end = in_file.seek(0, os.SEEK_END)

        # hop to the line with the first global URI subject
        chunk_end = seek_first_global_subject(args, in_file, file_end)

        with open(args.parts_file, 'w') as parts_file:
            tsv_writer = csv.writer(parts_file, delimiter='\t')
            part_number = 0

            while chunk_end < file_end:
                part_number += 1
                chunk_start = chunk_end

                # seek to the first line break after target
                in_file.seek(chunk_start + args.target_size)
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


def seek_first_global_subject(args, file_obj, file_end):
    id_marker = args.global_id_marker.encode('utf8')

    print('Looking for the first line with a global URI as subject:')
    if args.search_type == BINARY_SEARCH_TYPE:
        cursor = binary_search(args, file_obj, id_marker, file_end)
    else:
        cursor = jump_backpedal_and_step(args, file_obj, id_marker, file_end)

    return file_obj.seek(cursor)


def binary_search(args, file_obj, id_marker, file_end):
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


def jump_backpedal_and_step(args, file_obj, id_marker, file_end):
    subj_str = b''
    cursor = previous_jump_pos = 0

    try:
        # JUMP
        while id_marker not in subj_str and cursor < file_end:
            previous_jump_pos = cursor
            cursor, subj_str = seek_subject_at(file_obj, cursor, +args.jump_size)
            print('jump', cursor, subj_str)

        # BACKPEDAL
        while id_marker in subj_str and cursor > 0:
            cursor, subj_str = seek_subject_at(file_obj, cursor, -args.backpedal_size)
            print('backpedal', cursor, subj_str)
    except StopIteration:
        # jump or backpedal size is too small: step from previous jump
        cursor = previous_jump_pos

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
