from dbpedia.compute_parts import compute_parts
from dbpedia.preloader import parse_arguments
from dbpedia.utils import base_path


def get_test_args(**kwargs):
    args = parse_arguments(
        [],  # we don't want to test the parser by default
        output_dir=base_path('tests/output/'),
        **kwargs
    )
    return args


# TODO: clean output before each test


def test_skip_to_global_right_binary():
    args = get_test_args(
        input_path=base_path('samples/skip-to-right-test.nt'),
        target_size=500,
        search_type='binary',
    )
    part_positions = list(compute_parts(args))

    assert 2 == len(part_positions)
    assert (4615, 7243) == part_positions[0][1:]
    assert (7243, 7743) == part_positions[1][1:]


def test_skip_to_global_right_jump():
    args = get_test_args(
        input_path=base_path('samples/skip-to-right-test.nt'),
        target_size=500,
        search_type='jump',
        jump_size=1500,
    )
    part_positions = list(compute_parts(args))

    assert 2 == len(part_positions)
    assert (4615, 7243) == part_positions[0][1:]
    assert (7243, 7743) == part_positions[1][1:]


def test_skip_to_global_middle_binary():
    args = get_test_args(
        input_path=base_path('samples/skip-to-middle-test.nt'),
        target_size=500,
        search_type='binary',
    )
    part_positions = list(compute_parts(args))

    assert 3 == len(part_positions)
    assert (1834, 6236) == part_positions[0][1:]
    assert (6236, 6949) == part_positions[1][1:]
    assert (6949, 7676) == part_positions[2][1:]


def test_skip_to_global_middle_jump():
    args = get_test_args(
        input_path=base_path('samples/skip-to-middle-test.nt'),
        target_size=500,
        search_type='jump',
        jump_size=2500,
    )
    part_positions = list(compute_parts(args))

    assert 3 == len(part_positions)
    assert (1834, 6236) == part_positions[0][1:]
    assert (6236, 6949) == part_positions[1][1:]
    assert (6949, 7676) == part_positions[2][1:]


def test_skip_to_global_left_binary():
    args = get_test_args(
        input_path=base_path('samples/skip-to-left-test.nt'),
        target_size=500,
        search_type='binary',
    )
    part_positions = list(compute_parts(args))

    assert 2 == len(part_positions)
    assert (314, 1703) == part_positions[0][1:]
    assert (1703, 3587) == part_positions[1][1:]


def test_skip_to_global_left_jump():
    args = get_test_args(
        input_path=base_path('samples/skip-to-left-test.nt'),
        target_size=500,
        search_type='jump',
        jump_size=500,
        backpedal_size=300,
    )
    part_positions = list(compute_parts(args))

    assert 2 == len(part_positions)
    assert (314, 1703) == part_positions[0][1:]
    assert (1703, 3587) == part_positions[1][1:]


