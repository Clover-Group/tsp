CREATE TABLE IF NOT EXISTS events_wide_test
(
    series_storage Int32,
    app Int32,
    id Int32,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_narrow_test
(
    series_storage Int32,
    app Int32,
    id Int32,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_influx_test
(
    series_storage Int32,
    app Int32,
    id Int32,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();