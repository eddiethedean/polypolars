# Contributing to Polypolars

Thank you for considering contributing.

## Development setup

```bash
git clone https://github.com/eddiethedean/polypolars.git
cd polypolars
pip install -e ".[dev]"
```

## Running tests

```bash
pytest tests/ -v
```

With coverage:

```bash
pytest tests/ -v --cov=polypolars --cov-report=html
```

## Linting

```bash
ruff check polypolars tests examples benchmarks
```

## Pull requests

1. Fork the repo and create a branch from `main` (or `master`).
2. Make your changes and add or update tests.
3. Run tests and ruff.
4. Open a PR with a clear description. Reference any related issues.

## Code style

- Follow the existing style (ruff + 100 char line length).
- Prefer type hints for public APIs.
- Add docstrings for new public functions and classes.

## Reporting issues

Use the [issue tracker](https://github.com/eddiethedean/polypolars/issues). Include:

- Python and polars versions
- Minimal code to reproduce
- Expected vs actual behavior
