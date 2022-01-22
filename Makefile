.PHONY: format

format:
	poetry run python -m isort spellstore/ tests/
	poetry run python -m black spellstore tests

lint:
	poetry run python -m flake8 spellstore/ tests/
	poetry run python -m isort spellstore/ tests/ --check-only
	poetry run python -m black --check spellstore/ tests/
	poetry run python -m mypy spellstore/ tests/

test:
	poetry run python -m pytest --cov spellstore tests/ -vvv 