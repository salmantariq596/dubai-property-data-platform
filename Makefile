# Works on Windows (PowerShell / cmd with GNU make), Linux and macOS.
# All commands run through the project's virtual environment, so you don't
# need to activate it first.

ifeq ($(OS),Windows_NT)
    VENV_BIN := .venv/Scripts
else
    VENV_BIN := .venv/bin
endif

PY := $(VENV_BIN)/python

.PHONY: setup setup-spark lint format test test-fast profile-sample

setup:
	python -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[dev]"
	$(PY) -m pre_commit install

# Optional: local Spark + Delta for the bronze tests (needs Java 17+).
# On Windows, local Spark also needs winutils, so these tests are skipped
# there by default; they always run in CI.
setup-spark: setup
	$(PY) -m pip install -e ".[dev,spark]"

lint:
	$(PY) -m ruff check src tests
	$(PY) -m ruff format --check src tests

format:
	$(PY) -m ruff check --fix src tests
	$(PY) -m ruff format src tests

test:
	$(PY) -m pytest --cov=dubai_property --cov-report=term-missing

# Unit tests only: seconds, no Spark
test-fast:
	$(PY) -m pytest -m "not spark"

# Profile the bundled synthetic fixture (proves the tool works without real data)
profile-sample:
	$(PY) -m dubai_property.profiling tests/fixtures/sample_transactions.csv --key transaction_id --name "Sample transactions (synthetic)" --output docs/profiles/sample_transactions.md
