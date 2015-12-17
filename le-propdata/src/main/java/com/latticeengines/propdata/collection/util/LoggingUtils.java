package com.latticeengines.propdata.collection.util;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;

final public class LoggingUtils {

    public static String durationSince(long startTime) {
        return DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss:SSS");
    }

    public static void logInfo(Log log, ArchiveProgress progress, String message) {
        log.info(progressLogPrefix(progress) + message);
    }

    public static void logError(Log log, ArchiveProgress progress, String message,
                                                                       Exception e) {
        if (e == null) {
            log.error(progressLogPrefix(progress) + message);
        } else {
            log.error(progressLogPrefix(progress) + message, e);
        }
    }

    public static void logInfoWithDuration(Log log, ArchiveProgress progress,
                                                                                  String message, long startTime) {
        log.info(progressLogPrefix(progress) + message + " Duration=" + LoggingUtils.durationSince(startTime));
    }

    private static  String progressLogPrefix(ArchiveProgress progress) {
        return "Source=" + progress.getSourceName()
                + " RootOperationUID=" + progress.getRootOperationUID() + " ";
    }
}
