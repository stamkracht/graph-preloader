import os

from dbpedia.graph_elements import parse_arguments, transform_part, compute_parts


def base_path(name_or_path):
    base_dir = os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__)
        )
    )
    return os.path.join(base_dir, name_or_path)


def get_test_args(**kwargs):
    args = parse_arguments(
        [],  # we don't want to test the parser by default
        output_dir=base_path('tests/output/'),
        **kwargs
    )
    return args


# TODO: clean output before each test


def test_skip_to_global_2nd_half_binary():
    args = get_test_args(
        input_path=base_path('samples/skip-2nd-half-test.nt'),
        target_size=500,
        search_type='binary',
    )
    part_positions = list(compute_parts(args))

    assert 2 == len(part_positions)
    assert (4615, 7243) == part_positions[0][1:]
    assert (7243, 7743) == part_positions[1][1:]


def test_skip_to_global_2nd_half_jump():
    args = get_test_args(
        input_path=base_path('samples/skip-2nd-half-test.nt'),
        target_size=500,
        search_type='jump',
        jump_size=1500,
    )
    part_positions = list(compute_parts(args))

    assert 2 == len(part_positions)
    assert (4615, 7243) == part_positions[0][1:]
    assert (7243, 7743) == part_positions[1][1:]


def test_skip_to_global_between_binary():
    args = get_test_args(
        input_path=base_path('samples/skip-between-test.nt'),
        target_size=500,
        search_type='binary',
    )
    part_positions = list(compute_parts(args))

    assert 3 == len(part_positions)
    assert (1834, 6236) == part_positions[0][1:]
    assert (6236, 6949) == part_positions[1][1:]
    assert (6949, 7676) == part_positions[2][1:]


def test_skip_to_global_between_jump():
    args = get_test_args(
        input_path=base_path('samples/skip-between-test.nt'),
        target_size=500,
        search_type='jump',
        jump_size=2500,
    )
    part_positions = list(compute_parts(args))

    assert 3 == len(part_positions)
    assert (1834, 6236) == part_positions[0][1:]
    assert (6236, 6949) == part_positions[1][1:]
    assert (6949, 7676) == part_positions[2][1:]


def test_skip_to_global_long_tail_binary():
    args = get_test_args(
        input_path=base_path('samples/skip-long-tail-test.nt'),
        target_size=500,
        search_type='binary',
    )
    part_positions = list(compute_parts(args))

    assert 2 == len(part_positions)
    assert (314, 1703) == part_positions[0][1:]
    assert (1703, 3587) == part_positions[1][1:]


def test_skip_to_global_long_tail_jump():
    args = get_test_args(
        input_path=base_path('samples/skip-long-tail-test.nt'),
        target_size=500,
        search_type='jump',
        jump_size=500,
        backpedal_size=300,
    )
    part_positions = list(compute_parts(args))

    assert 2 == len(part_positions)
    assert (314, 1703) == part_positions[0][1:]
    assert (1703, 3587) == part_positions[1][1:]


