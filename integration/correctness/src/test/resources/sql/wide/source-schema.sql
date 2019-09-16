CREATE TABLE IF NOT EXISTS Test.SM_basic_wide (
    datetime Double,
    series_id String,
    mechanism_id String,
    speed Float32
) ENGINE = Memory();


CREATE TABLE IF NOT EXISTS Test.SM_typeCasting_wide (
    datetime Double,
    series_id String,
    mechanism_id String,
    speed Int32,
    speed64 Float64
) ENGINE = Memory();


CREATE TABLE IF NOT EXISTS Test.SM_test_wide (
    datetime Double,
    series_id String,
    mechanism_id String,
    speed Float64,
    speed64 Float64
) ENGINE = Memory();