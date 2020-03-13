CREATE TABLE IF NOT EXISTS events_wide_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_narrow_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_influx_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_narrow_ivolga_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_wide_ivolga_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_wide_kafka_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_wide_spark_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_narrow_spark_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_wide_ivolga_spark_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();
CREATE TABLE IF NOT EXISTS events_wide_kafka_spark_test
(
    series_storage Int32,
    app Int32,
    id UInt64,
    "timestamp" Float64,
    from Float64,
    to Float64,
    context String
) ENGINE = Log();