VENV_NAME:=.venv
REQS_PROD_IN:=requirements/prod.in
REQS_PROD_TXT:=requirements.txt
REQS_ALL:=requirements/all.txt

## help: Prints this list of commands.
## gcp: pulls gcloud docker image, authenticates to google cloud and configure the project.
## build: Builds docker image.
## login: run google cloud authentication .
## docker-shell: Enters to docker container shell.
## requirements: Compiles requirements file with pip-tools.
## upgrade-requirementsde: Upgrades requirements file based on .in constraints.
## venv: creates a virtual environment inside .venv.
## venv3.8: creates a virtual environment inside .venv (using python3.8).
## install: Installs all dependencies needed for development.
## test: Runs unit tests.
## testdocker: Runs unit tests inside docker container.
## testdocker-all: Runs unit and integration tests inside docker container.
## docker-flake: Checks PEP8 code style agreement.


help:
	@echo "\nUsage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/-/'

gcp:
	docker compose pull gcloud
	docker volume create --name=gcp
	docker compose run --rm gcloud auth application-default login
	docker compose run --rm gcloud config set project world-fishing-827
	docker compose run --rm gcloud auth application-default set-quota-project world-fishing-827

build:
	docker compose build

docker-shell:
	docker compose run --rm --entrypoint /bin/bash -it dev

requirements:
	docker compose run --rm --entrypoint /bin/bash -it dev -c \
		'pip-compile -o ${REQS_PROD_TXT} ${REQS_PROD_IN} -v'

upgrade-requirements:
	docker compose run --rm --entrypoint /bin/bash -it dev -c \
	'pip-compile -o ${REQS_PROD_TXT} -U ${REQS_PROD_IN} -v'

venv:
	python3 -m venv ${VENV_NAME}

venv3.8:
	python3.8 -m venv ${VENV_NAME}

install:
	pip install -r ${REQS_ALL}

test:
	pytest

testdocker:
	docker compose run --rm --entrypoint pytest dev

testdocker-all:
	docker compose run --rm --entrypoint "pytest --runslow" dev

docker-flake:
	docker compose run --rm --entrypoint flake8 -it dev --count


.PHONY: help gcp build dockersheel requirements upgrade-requirements venv venv3.8 install test testdocker testdocker-all docker-flake
