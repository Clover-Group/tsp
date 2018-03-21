CREATE TABLE IF NOT EXISTS Test.SM_basic_wide_patterns ( date Date DEFAULT now(),  from DateTime,  from_millis UInt16 DEFAULT 0,  to DateTime,  to_millis UInt16 DEFAULT 0,  pattern_id String,  mechanism_id String) ENGINE = Log();

CREATE TABLE IF NOT EXISTS Test.SM_basic_narrow_patterns ( date Date DEFAULT now(),  from DateTime,  from_millis UInt16 DEFAULT 0,  to DateTime,  to_millis UInt16 DEFAULT 0,  pattern_id String,  mechanism_id String) ENGINE = Log()
