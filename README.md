# graph-preloader
Transforming triples into objects and objects into better objects

## running
This project uses the [pipenv](https://docs.pipenv.org/) tool to manage the
virtual environment. To run the scripts included here, install pipenv if
needed, install dependencies, and then enter an environment shell:

    pip install pipenv
    pipenv install --dev
    pipenv shell

single commands can also be run in the enviroment without spawning a full
shell:

    pipenv run my_script.py

check the pipenv documentation for instructions on how to add or remove
requirements.

## wikidata

There are several scripts for pulling, separating, and transforming wikidata
objects in the wikidata directory. They all come with a short summary at the
top. Scripts that require input take it from stdin. Output is written to
stdout, unless output needs to be separated into multiple files, in which case
a `data/` directory is expected where output files can be written to.
Specifying output filenames is a possible future enhancement.

## dbpedia

The DBpedia preloader tool can be used as follows:

    $ pipenv run python -m dbpedia.preloader -h
    usage: preloader.py [-h] [-p] [-su] [-sts STS_URL] [-ts TARGET_SIZE] [-v VL]
                        [--global-id-marker GLOBAL_ID_MARKER]
                        [--id-marker-prefix ID_MARKER_PREFIX]
                        [--parts-file PARTS_FILE] [--task-timeout TASK_TIMEOUT]
                        [--search-type {binary,jump}]
                        [--bin-search-limit BIN_SEARCH_LIMIT]
                        [--jump-size JUMP_SIZE] [--backpedal-size BACKPEDAL_SIZE]
                        [input_path] [output_dir]
    
    Transform sorted Databus NTriples into property graph-friendly JSON.
    
    positional arguments:
      input_path            the Databus NTriples input file path (default:
                            /home/alex/Qollap/graph-preloader/dbpedia/sorted.nt)
      output_dir            the JSON output directory path (default:
                            /home/alex/Qollap/graph-
                            preloader/dbpedia/output_5c925350/)
    
    optional arguments:
      -h, --help            show this help message and exit
      -p, --parallel        transform parts in parallel using a multiprocessing
                            pool (default: False)
      -su, --shorten-uris   shorten URIs by replacing known namespaces with their
                            corresponding prefix (default: False)
      -sts STS_URL, --samething-service STS_URL
                            the base URL of a DBpedia Same Thing Service endpoint,
                            e.g. http://downloads.dbpedia.org/same-thing/
                            (default: None)
      -ts TARGET_SIZE, --target-size TARGET_SIZE
                            the approximate size of parts in bytes (default:
                            500e6)
      -v VL, --verbosity VL
                            verbosity level for messages printed to stdout &
                            stderr (default: 1)
      --global-id-marker GLOBAL_ID_MARKER
                            only triples with this marker in the subject will be
                            transformed (default: global.dbpedia.org/id/)
      --id-marker-prefix ID_MARKER_PREFIX
                            the characters that precede the `global_id_marker` in
                            each triple (default: <https://)
      --parts-file PARTS_FILE
                            the file in which output files are listed with
                            corresponding input file positions (left and right)
                            (default: <output_dir>/parts.tsv)
      --task-timeout TASK_TIMEOUT
                            the number of seconds a "transform part" task is
                            allowed to run (applies only to parallel execution)
                            (default: 600)
      --search-type {binary,jump}
                            the type of search to use to skip to the first
                            `global_id_marker` triple (default: binary)
      --bin-search-limit BIN_SEARCH_LIMIT
                            the maximum number of iterations of the binary search
                            main loop (default: 120)
      --jump-size JUMP_SIZE
                            the size of forward jumps in bytes (default: 350e6)
      --backpedal-size BACKPEDAL_SIZE
                            the size of backpedals in bytes (default: <jump_size>
                            // 10)

