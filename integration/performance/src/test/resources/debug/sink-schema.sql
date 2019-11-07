CREATE TABLE IF NOT EXISTS events_ep2k
(
    series_storage Int32,
    "type" Int32,
    entity_id Int32,
    processing_ts Float64,
    from_ts Float64,
    to_ts Float64,
    context String
) ENGINE = Log();
