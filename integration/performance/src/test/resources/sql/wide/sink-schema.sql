CREATE TABLE IF NOT EXISTS Test.SM_basic_wide_patterns
(
    series_storage Int32,
    app Int32,
    id Int32,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
