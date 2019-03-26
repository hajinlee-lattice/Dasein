package com.latticeengines.apps.cdl.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ImportMessageUtils {

    private static Logger log = LoggerFactory.getLogger(S3ImportMessageUtils.class);

    private static String FEED_TYPE_PATTERN = "%s_%s";
    private static final String TEMPLATES = "Templates";

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

    public static String getSystemNameFromKey(String key) {
        if (StringUtils.isEmpty(key)) {
            return StringUtils.EMPTY;
        }
        String[] parts = key.split("/");
        if (parts.length < 5) {
            throw new IllegalArgumentException(String.format("Cannot parse key %s", key));
        }
        if (parts.length == 5) {
            return StringUtils.EMPTY;
        } else if (parts.length == 6) {
            return parts[2];
        } else {
            throw new IllegalArgumentException(String.format("Cannot parse key %s", key));
        }
    }

    public static String formatFeedType(String systemName, String folderName) {
        if (StringUtils.isNotEmpty(systemName))
            return String.format(FEED_TYPE_PATTERN, systemName, folderName);
        return folderName;
    }

}
