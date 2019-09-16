CREATE TABLE IF NOT EXISTS Test.SM_basic_narrow (
    datetime Double,
    series_id String,
    mechanism_id String,
    key String,
    value Float32
) ENGINE = Memory();