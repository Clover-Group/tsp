package ru.itclover.tsp;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class DebugTsViolationHandler implements AscendingTimestampExtractor.MonotonyViolationHandler {
    private static final long serialVersionUID = 2L;

    private static final Logger LOG = LoggerFactory.getLogger(AscendingTimestampExtractor.class);

    @Override
    public void handleViolation(long elementTimestamp, long lastTimestamp) {
        LOG.debug("Timestamp monotony violated: {} < {}", elementTimestamp, lastTimestamp);
    }
}
