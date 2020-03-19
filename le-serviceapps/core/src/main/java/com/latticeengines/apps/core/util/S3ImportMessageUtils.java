package com.latticeengines.apps.core.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

public final class S3ImportMessageUtils {

    protected S3ImportMessageUtils() {
        throw new UnsupportedOperationException();
    }

    private static String FEED_TYPE_PATTERN = "%s_%s";
    private static final String PS_SHARE = "PS_SHARE";
    // DCP key : dropfolder/{dropbox}/Projects/{ProjectId}/Source/{SourceId}/drop/{fileName}
    private static final Pattern DCP_PATTERN = Pattern.compile("dropfolder/([a-zA-Z0-9]{8})/Projects/([a-zA-Z0-9_]+)/Source/([a-zA-Z0-9_]+)/drop/(.*)");
    private static final Pattern ATLAS_PATTERN = Pattern.compile("dropfolder/([a-zA-Z0-9]{8})/Templates/(.*)");
    private static final Pattern LEGACY_ATLAS_PATTERN = Pattern.compile("dropfolder/([a-zA-Z0-9]{8})/([a-zA-Z0-9_]+)/Templates/(.*)");

    public static S3ImportMessageType getMessageTypeFromKey(String key) {
        if (StringUtils.isEmpty(key)) {
            return S3ImportMessageType.UNDEFINED;
        }
        String[] parts = key.split("/");
        if (parts.length < 4 || !key.toLowerCase().endsWith(".csv")) {
            return S3ImportMessageType.UNDEFINED;
        } else {
            if (DCP_PATTERN.matcher(key).find()) {
                return S3ImportMessageType.DCP;
            } else if (ATLAS_PATTERN.matcher(key).find() || LEGACY_ATLAS_PATTERN.matcher(key).find()) {
                return S3ImportMessageType.Atlas;
            } else {
                return S3ImportMessageType.UNDEFINED;
            }
        }
    }

    public static boolean shouldSkipMessage(String key, S3ImportMessageType messageType) {
        boolean skip = false;
        switch (messageType) {
            case Atlas:
                String feedType = getFeedTypeFromKey(key);
                skip = PS_SHARE.equals(feedType);
                break;
            case DCP:
                break;
            default:
                skip = true;
                break;
        }
        return skip;
    }

    public static String getKeyPart(String key, S3ImportMessageType messageType, KeyPart keyPart) {
        if (S3ImportMessageType.DCP.equals(messageType)) {
            Matcher matcher = DCP_PATTERN.matcher(key);
            if (matcher.find()) {
                String result;
                switch (keyPart) {
                    case PROJECT_ID:
                        result = matcher.group(2);
                        break;
                    case SOURCE_ID:
                        result = matcher.group(3);
                        break;
                    case FILE_NAME:
                        result = matcher.group(4);
                        break;
                    default:
                        result = StringUtils.EMPTY;
                        break;
                }
                return result;
            } else {
                return StringUtils.EMPTY;
            }
        } else {
            throw new NotImplementedException("Message type: " + messageType + " is not supported yet.");
        }
    }

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

    public enum KeyPart {
        PROJECT_ID, SOURCE_ID, FILE_NAME
    }

}
