VENV_NAME:=.venv
PYTHON=${VENV_NAME}/bin/python3


## help: Prints this list of commands.
## venv: creates a virtual environment inside .venv.
## build: Builds docker image.
## install: Installs all dependencies needed for development.
## requirements: Compiles requirements file with pip-tools.
## upgrade-requirementsde: Upgrades requirements file based on .in constraints.
## test: Runs unit tests.
## testdocker: Runs unit tests inside docker container.
## testdocker-all: Runs unit and integration tests inside docker container.



help:
	@echo "\nUsage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/-/'

venv:
	python3 -m venv ${VENV_NAME}

build:
	docker compose build

install:
	pip install -r requirements/all.txt

requirements: requirements/prod.in
	pip-compile -o requirements.txt requirements/prod.in -v

upgrade-requirements:
	pip-compile -o requirements.txt -U requirements/prod.in -v

test:
	pytest

testdocker:
	docker compose run --entrypoint pytest dev

testdocker-all:
	docker compose run --entrypoint "pytest --runslow" dev


PHONY: help install requirements upgrade-requirements test testdocker testdocker-all