.PHONY: format

format:
	poetry run python -m isort spellbook/ tests/
	poetry run python -m black spellbook tests

lint:
	poetry run python -m flake8 spellbook/ tests/
	poetry run python -m isort spellbook/ tests/ --check-only
	poetry run python -m black --check spellbook/ tests/
	poetry run python -m mypy spellbook/ tests/

test:
	poetry run python -m pytest tests/ -vvv