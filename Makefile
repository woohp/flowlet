.PHONY: format fformat lint test

format:
	uv run ruff check --fix-only --ignore F401 .
	uv run ruff format .

fformat:
	uv run ruff check --fix-only .
	uv run ruff format .

lint:
	uv run ruff check .

test:
	uv run pytest
