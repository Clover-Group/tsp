CREATE TABLE IF NOT EXISTS SM_Integration_wide (
    datetime DateTime,
    mechanism_id String,
    speed Float32
) ENGINE = Log()