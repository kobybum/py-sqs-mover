
test-and-lint:
	@pytest --disable-warnings
	@mypy --ignore-missing-imports .
	@black --check -l 100 .
	@flake8 . --max-line-length=100

format-code:
	@black -l 100 .
