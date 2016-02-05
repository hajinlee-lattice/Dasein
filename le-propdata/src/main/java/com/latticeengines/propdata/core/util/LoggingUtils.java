package com.latticeengines.propdata.core.util;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;

import com.latticeengines.domain.exposed.propdata.manage.Progress;
import com.latticeengines.propdata.match.service.impl.MatchContext;

final public class LoggingUtils {

    public static String durationSince(long startTime) {
        return DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss.SSS");
    }

    public static <P extends Progress> void logInfo(Log log, P progress, String message) {
        log.info(progressLogPrefix(progress) + message);
    }

    public static <P extends Progress> void logError(Log log, P progress, String message, Exception e) {
        if (e == null) {
            log.error(progressLogPrefix(progress) + message);
        } else {
            log.error(progressLogPrefix(progress) + message, e);
        }
    }

    public static <P extends Progress> void logInfoWithDuration(Log log, P progress, String message, long startTime) {
        log.info(progressLogPrefix(progress) + message + " Duration=" + LoggingUtils.durationSince(startTime));
    }

    private static <P extends Progress> String progressLogPrefix(P progress) {
        return "Source=" + progress.getSourceName() + " RootOperationUID=" + progress.getRootOperationUID() + " ";
    }

    public static void logError(Log log, String message, MatchContext matchContext, Exception e) {
        if (e == null) {
            log.error(message + " RootOperationUID=" + matchContext.getOutput().getRootOperationUID());
        } else {
            log.error(message + " RootOperationUID=" + matchContext.getOutput().getRootOperationUID(), e);
        }
    }
}
