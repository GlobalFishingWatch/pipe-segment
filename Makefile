VENV_NAME:=.venv
REQS_WORKER:=requirements/worker
REQS_SCHEDULER:=requirements/scheduler

## help: Prints this list of commands.
## build: Builds docker image.
## dockershell: Enters to docker container shell.
## venv: creates a virtual environment inside .venv.
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

build:
	docker compose build

dockershell:
	docker compose run --entrypoint /bin/bash -it dev

requirements-worker: requirements/worker.in
	docker compose run --entrypoint /bin/bash -it dev -c \
		'pip-compile -o ${REQS_WORKER}.txt ${REQS_WORKER}.in -v && \
		pip-compile -o ${REQS_SCHEDULER}.txt ${REQS_SCHEDULER}.in -v' \

requirements-scheduler: requirements/scheduler.in
	docker compose run --entrypoint /bin/bash -it dev -c \
		'pip-compile -o ${REQS_SCHEDULER}.txt ${REQS_SCHEDULER}.in -v'

upgrade-requirements:
	docker compose run --entrypoint /bin/bash -it dev -c \
	'pip-compile -o ${REQS_WORKER}.txt -U ${REQS_WORKER}.in -v && \
	pip-compile -o ${REQS_SCHEDULER}.txt -U ${REQS_SCHEDULER}.in -v'

venv:
	python3 -m venv ${VENV_NAME}

venv3.8:
	python3.8 -m venv ${VENV_NAME}

install:
	pip install -r requirements/all.txt

test:
	pytest

testdocker:
	docker compose run --entrypoint pytest dev

testdocker-all:
	docker compose run --entrypoint "pytest --runslow" dev


PHONY: help install requirements requirements-upgrade requirements-worker requirements-scheduler test testdocker testdocker-all