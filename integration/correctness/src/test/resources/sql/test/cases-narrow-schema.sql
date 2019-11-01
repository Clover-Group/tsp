CREATE TABLE default.math_test (`date_part` Date DEFAULT CAST(now(), 'Date'), `dt` Float64, `ms` Float32, `upload` DateTime, `sensor_id` String, `value_float` Float32, `loco_num` String, `section` String, `bind_model_id` Int64, `upload_id` String, `ver` Int8 DEFAULT CAST(1, 'Int8'), `math_model_build` UInt32) ENGINE = ReplacingMergeTree(ver) PARTITION BY toMonday(toDateTime(dt)) ORDER BY (loco_num, section, upload_id, sensor_id, dt) SAMPLE BY dt SETTINGS index_granularity = 8192
