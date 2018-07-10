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
