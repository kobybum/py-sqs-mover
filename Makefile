
lint:
	@mypy --ignore-missing-imports .
	@black --check -l 100 .
	@flake8 . --max-line-length=100

test:
	@pytest
