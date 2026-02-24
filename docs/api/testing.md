# API: Testing

- **assert_dataframe_equal**(df1, df2, check_order=False, rtol=1e-5, atol=1e-8, check_column_order=False)
- **assert_schema_equal**(schema1, schema2, check_order=False)
- **assert_approx_count**(df, expected_count, tolerance=0.1)
- **assert_column_exists**(df, *columns)
- **assert_no_duplicates**(df, columns=None)
- **get_column_stats**(df, column) → dict

**DataFrameComparisonError** is raised when an assertion fails.
