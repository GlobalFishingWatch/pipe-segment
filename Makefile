VENV_NAME:=.venv
PYTHON=${VENV_NAME}/bin/python3


## help: Prints this list of commands.
## install: Installs development dependencies.
## requirements: Compiles requirement txt files with pip-tools.
## upgrade-requirements: Upgrades requirements txt files based on .in constraints.
## requirements-worker: Compiles only worker requirements with pip-tools.
## requirements-scheduler: Compiles only scheduler requirements with pip-tools.
## test: Run unit tests.
## testdocker: Run unit tests inside docker container.
## testdocker-all: Run unit and integration tests inside docker container.



help:
	@echo "\nUsage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/-/'

install:
	pip install -r requirements/dev.txt

requirements: requirements-worker requirements-scheduler

requirements-worker: requirements/worker.in
	pip-compile -o requirements/worker.txt requirements/worker.in -v

requirements-scheduler: requirements/scheduler.in
	pip-compile -o requirements/scheduler.txt requirements/scheduler.in -v

upgrade-requirements:
	pip-compile -o requirements/worker.txt -U requirements/scheduler.in -v
	pip-compile -o requirements/scheduler.txt -U requirements/scheduler.in -v

test:
	pytest

testdocker:
	docker compose run --entrypoint pytest dev

testdocker-all:
	docker compose run --entrypoint "pytest --runslow" dev


PHONY: help install requirements requirements-upgrade requirements-worker requirements-scheduler test testdocker testdocker-all