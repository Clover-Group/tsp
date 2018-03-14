CREATE TABLE IF NOT EXISTS SM_basic_wide (
    datetime DateTime,
    mechanism_id String,
    speed Float32
) ENGINE = Log();


CREATE TABLE IF NOT EXISTS SM_typeCasting_wide (
    datetime DateTime,
    mechanism_id String,
    speed Int32
) ENGINE = Log();
