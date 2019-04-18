package com.latticeengines.datacloud.core.util;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;

import com.latticeengines.domain.exposed.datacloud.manage.Progress;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

public final class LoggingUtils {

    public static String durationSince(long startTime) {
        return DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss.SSS");
    }

    public static void logInfo(Logger log, TransformationProgress progress, String message) {
        log.info(progressLogPrefix(progress) + message);
    }

    public static void logError(Logger log, TransformationProgress progress, String message, Exception e) {
        if (e == null) {
            log.error(progressLogPrefix(progress) + message);
        } else {
            log.error(progressLogPrefix(progress) + message, e);
        }
    }

    public static <P extends Progress> void logInfo(Logger log, P progress, String message) {
        log.info(progressLogPrefix(progress) + message);
    }

    public static <P extends Progress> void logError(Logger log, P progress, String message, Exception e) {
        if (e == null) {
            log.error(progressLogPrefix(progress) + message);
        } else {
            log.error(progressLogPrefix(progress) + message, e);
        }
    }

    public static <P extends Progress> void logInfoWithDuration(Logger log, P progress, String message, long startTime) {
        log.info(progressLogPrefix(progress) + message + " Duration=" + LoggingUtils.durationSince(startTime));
    }

    public static void logInfoWithDuration(Logger log, TransformationProgress progress, String message, long startTime) {
        log.info(progressLogPrefix(progress) + message + " Duration=" + LoggingUtils.durationSince(startTime));
    }

    private static String progressLogPrefix(TransformationProgress progress) {
        return "Source=" + progress.getSourceName() + " RootOperationUID=" + progress.getRootOperationUID() + " ";
    }

    private static <P extends Progress> String progressLogPrefix(P progress) {
        return "Source=" + progress.getSourceName() + " RootOperationUID=" + progress.getRootOperationUID() + " ";
    }
}
