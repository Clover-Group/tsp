CREATE TABLE IF NOT EXISTS SM_basic_wide (
    datetime DateTime,
    mechanism_id String,
    speed Float32
) ENGINE = Log();


CREATE TABLE IF NOT EXISTS SM_basic_narrow (
    datetime DateTime,
    mechanism_id String,
    sensor String,
    value_float Float32
) ENGINE = Log();
