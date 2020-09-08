CREATE TABLE IF NOT EXISTS Test.SM_basic_patterns
(
    series_storage Int32,
    app Int32,
    id Int32,
    subunit Int32,
    from Float64,
    to Float64
) ENGINE = Log();
