CREATE TABLE IF NOT EXISTS Test.SM_basic_wide (
    datetime DateTime,
    mechanism_id String,
    speed Float32
) ENGINE = Log();

CREATE TABLE Test."EventsSink"
(
    series_storage Int32,
    app Int32,
    id Int32,
    "timestamp" Float64,
    begin Float64,
    "end" Float64,
    context String
) ENGINE = Log();


CREATE TABLE IF NOT EXISTS Test.SM_typeCasting_wide (
    datetime DateTime,
    mechanism_id String,
    speed Int32
) ENGINE = Log();
