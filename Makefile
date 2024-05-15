VENV_NAME:=.venv
PYTHON=${VENV_NAME}/bin/python3


install:
	pip install -r requirements/dev.txt

requirements: requirements-worker requirements-scheduler

requirements-worker: requirements/worker.in
	pip-compile -o requirements/worker.txt requirements/worker.in -v

requirements-scheduler: requirements/scheduler.in
	pip-compile -o requirements/scheduler.txt requirements/scheduler.in -v

test:
	pytest

testdocker:
	docker compose run --entrypoint pytest dev

testdocker-all:
	docker compose run --entrypoint "pytest --runslow" dev


PHONY: requirements requirements-worker requirements-scheduler test testdocker testdocker-all