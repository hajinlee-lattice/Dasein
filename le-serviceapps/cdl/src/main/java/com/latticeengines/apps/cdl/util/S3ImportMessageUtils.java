package com.latticeengines.apps.cdl.util;

import org.apache.commons.lang3.StringUtils;

public final class S3ImportMessageUtils {

    protected S3ImportMessageUtils() {
        throw new UnsupportedOperationException();
    }

    private static String FEED_TYPE_PATTERN = "%s_%s";

    public static String getFeedTypeFromKey(String key) {
        if (StringUtils.isEmpty(key)) {
            return StringUtils.EMPTY;
        }
        String[] parts = key.split("/");
        if (parts.length < 5) {
            throw new IllegalArgumentException(String.format("Cannot parse key %s", key));
        }
        if (parts.length == 5) {
            return parts[3];
        } else if (parts.length == 6) {
            return String.format(FEED_TYPE_PATTERN, parts[2], parts[4]);
        } else {
            throw new IllegalArgumentException(String.format("Cannot parse key %s", key));
        }
    }

    public static String getFileNameFromKey(String key) {
        if (StringUtils.isEmpty(key)) {
            return StringUtils.EMPTY;
        }
        String[] parts = key.split("/");
        if (parts.length < 5) {
            throw new IllegalArgumentException(String.format("Cannot parse key %s", key));
        }
        return parts[parts.length - 1];
    }

    public static String getDropBoxPrefix(String key) {
        if (StringUtils.isEmpty(key)) {
            return StringUtils.EMPTY;
        }
        String[] parts = key.split("/");
        if (parts.length < 5) {
            throw new IllegalArgumentException(String.format("Cannot parse key %s", key));
        }
        return parts[1];
    }

    public static String formatFeedType(String systemName, String folderName) {
        if (StringUtils.isNotEmpty(systemName))
            return String.format(FEED_TYPE_PATTERN, systemName, folderName);
        return folderName;
    }

}
