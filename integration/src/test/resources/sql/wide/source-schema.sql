CREATE TABLE IF NOT EXISTS Test.SM_basic_wide (
    datetime Double,
    series_id String,
    mechanism_id String,
    speed Float32
) ENGINE = Log();


CREATE TABLE IF NOT EXISTS Test.SM_typeCasting_wide (
    datetime Double,
    series_id String,
    mechanism_id String,
    speed Int32
) ENGINE = Log();
