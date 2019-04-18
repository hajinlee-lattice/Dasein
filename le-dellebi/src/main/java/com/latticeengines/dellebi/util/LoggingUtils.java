package com.latticeengines.dellebi.util;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;

import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLogStatus;

public final class LoggingUtils {

    public static String durationSince(long startTime) {
        return DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss.SSS");
    }

    public static void logInfo(Logger log, DellEbiExecutionLog progress, String message) {
        log.info(progressLogPrefix(progress) + message);
    }

    public static void logErrorWithDuration(Logger log, DellEbiExecutionLog progress, String message, Exception e,
            long startTime) {
        logError(log, progress, message + " Duration=" + LoggingUtils.durationSince(startTime), e);
    }

    public static void logError(Logger log, DellEbiExecutionLog progress, String message, Exception e) {
        if (e == null) {
            log.error(progressLogPrefix(progress) + message);
        } else {
            log.error(progressLogPrefix(progress) + message, e);
        }
    }

    public static void logInfoWithDuration(Logger log, DellEbiExecutionLog progress, String message, long startTime) {
        log.info(progressLogPrefix(progress) + message + " Duration=" + LoggingUtils.durationSince(startTime));
    }

    private static String progressLogPrefix(DellEbiExecutionLog progress) {
        String status = DellEbiExecutionLogStatus.getStatusNameByCode(progress.getStatus());
        return "File=" + progress.getFile() + " Status=" + status + " ";
    }
}
