package com.latticeengines.domain.exposed.cdl.activity;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.latticeengines.common.exposed.util.DateTimeUtils;

public class EmptyStreamException extends RuntimeException {
    private static final long serialVersionUID = -952967710809464171L;
    private static final String ERR_MSG_FORMAT = "%s time series data are all out of retention period"
            + " (%s - %s). Please import data within specified date range.";

    public EmptyStreamException(String streamName, Integer nRetentionDays, long currentEpochMilli) {
        super(formatErrorMessage(streamName, nRetentionDays, currentEpochMilli));
    }

    public EmptyStreamException(String message) {
        super(message);
    }

    private static String formatErrorMessage(String streamName, Integer nRetentionDays, long currentEpochMilli) {
        if (nRetentionDays != null) {
            long retentionBoundryTimestamp = Instant.ofEpochMilli(currentEpochMilli) //
                    .minus(nRetentionDays, ChronoUnit.DAYS) //
                    .toEpochMilli();
            String startDate = DateTimeUtils.toDateOnlyFromMillis(String.valueOf(retentionBoundryTimestamp));
            String endDate = DateTimeUtils.toDateOnlyFromMillis(String.valueOf(currentEpochMilli));
            return String.format(ERR_MSG_FORMAT, streamName, startDate, endDate);
        } else {
            return String.format("No %s time series data imported", streamName);
        }
    }
}
