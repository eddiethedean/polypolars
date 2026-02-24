# API: I/O

## Save

- **save_as_parquet**(df, path, **options)
- **save_as_json**(df, path, **options)
- **save_as_csv**(df, path, header=True, **options)
- **save_dicts_as_json**(data, path)

## Load

- **load_parquet**(path, **options) → DataFrame
- **load_json**(path, **options) → DataFrame
- **load_csv**(path, has_header=True, **options) → DataFrame
- **load_dicts_from_json**(path) → List[Dict]
- **load_and_validate**(path, expected_schema=None, validate_schema=True) → DataFrame
