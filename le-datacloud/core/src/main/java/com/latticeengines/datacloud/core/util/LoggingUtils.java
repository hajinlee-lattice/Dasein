package com.latticeengines.datacloud.core.util;

import org.apache.commons.lang3.time.DurationFormatUtils;

import com.latticeengines.domain.exposed.datacloud.manage.Progress;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

public final class LoggingUtils {

    protected LoggingUtils() {
        throw new UnsupportedOperationException();
    }

    public static String log(String clzName, String message) {
        return "[" + clzName + "] " + message;
    }

    public static <P extends Progress> String log(String clzName, P progress, String message) {
        return progressLogPrefix(clzName, progress) + message;
    }

    public static String log(String clzName, TransformationProgress progress, String message) {
        return progressLogPrefix(clzName, progress) + message;
    }

    public static <P extends Progress> String logWithDuration(String clzName, P progress, String message,
            long startTime) {
        return progressLogPrefix(clzName, progress) + message + " Duration=" + LoggingUtils.durationSince(startTime);
    }

    private static String progressLogPrefix(String clzName, TransformationProgress progress) {
        return String.format("[%s] Source=%s RootOperationUID=%s ", clzName, progress, progress.getRootOperationUID());
    }

    private static <P extends Progress> String progressLogPrefix(String clzName, P progress) {
        return String.format("[%s] Source=%s RootOperationUID=%s ", clzName, progress, progress.getRootOperationUID());
    }

    private static String durationSince(long startTime) {
        return DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss.SSS");
    }

}
