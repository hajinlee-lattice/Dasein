package com.latticeengines.propdata.collection.util;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressBase;

final public class LoggingUtils {

    public static String durationSince(long startTime) {
        return DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss:SSS");
    }

    public static <Progress extends ArchiveProgressBase> void logInfo(Log log, Progress progress, String message) {
        log.info(progressLogPrefix(progress) + message);
    }

    public static <Progress extends ArchiveProgressBase> void logError(Log log, Progress progress, String message,
                                                                       Exception e) {
        if (e == null) {
            log.error(progressLogPrefix(progress) + message);
        } else {
            log.error(progressLogPrefix(progress) + message, e);
        }
    }

    public static <Progress extends ArchiveProgressBase> void logInfoWithDuration(Log log, Progress progress,
                                                                                  String message, long startTime) {
        log.info(progressLogPrefix(progress) + message + " Duration=" + LoggingUtils.durationSince(startTime));
    }

    private static  <Progress extends ArchiveProgressBase> String progressLogPrefix(Progress progress) {
        return "Progress=" + progress.getClass().getSimpleName()
                + " RootOperationUID=" + progress.getRootOperationUID() + " ";
    }
}
