CREATE TABLE IF NOT EXISTS SM_Integration_wide (
    datetime DateTime,
    mechanism_id String,
    speed Float32
) ENGINE = Log();


CREATE TABLE IF NOT EXISTS SM_Integration_narrow (
    datetime DateTime,
    mechanism_id String,
    sensor_name String,
    fvalue Float32
) ENGINE = Log();
