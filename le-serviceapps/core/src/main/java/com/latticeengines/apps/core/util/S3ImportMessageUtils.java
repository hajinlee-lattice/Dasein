package com.latticeengines.apps.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

public final class S3ImportMessageUtils {

    protected S3ImportMessageUtils() {
        throw new UnsupportedOperationException();
    }

    private static String FEED_TYPE_PATTERN = "%s_%s";
    private static final String PS_SHARE = "PS_SHARE";
    public static final String ENTERPRISE_INTEGRATION = "enterprise_integration";
    // DCP key : dropfolder/{dropbox}/Projects/{ProjectId}/Sources/{SourceId}/drop/{fileName}
    private static final Pattern DCP_PATTERN = Pattern.compile("dropfolder/([a-zA-Z0-9]{8})/Projects/([a-zA-Z0-9_]+)/Source[s]?/([a-zA-Z0-9_]+)/drop/(.*)");
    private static final Pattern ATLAS_PATTERN = Pattern.compile("dropfolder/([a-zA-Z0-9]{8})/Templates/(.*)");
    private static final Pattern LEGACY_ATLAS_PATTERN = Pattern.compile("dropfolder/([a-zA-Z0-9]{8})/([a-zA-Z0-9_]+)/Templates/(.*)");
    private static final Pattern LIST_SEGMENT_PATTERN = Pattern.compile("datavision_segment/([a-zA-Z0-9_]+)/([a-zA-Z0-9_]+)/(.*)");
    private static final Pattern DATA_OPERATION_PATTERN = Pattern.compile("dropfolder/([a-zA-Z0-9]{8})/Data_Operation/(.*)");
    private static final Pattern ENTERPRISE_INTEGRATION_PATTERN = Pattern.compile(ENTERPRISE_INTEGRATION + "/([a-zA-Z0-9_]+)/([a-zA-Z0-9_-]+)/([a-zA-Z0-9_]+)/(.*)");
    public static final List<String> validImportFileTypes = Lists.newArrayList(".csv", ".gzip", ".gz", ".tar", ".tar.gz", ".zip");

    public static S3ImportMessageType getMessageTypeFromKey(String key) {
        if (StringUtils.isEmpty(key)) {
            return S3ImportMessageType.UNDEFINED;
        }
        String[] parts = key.split("/");
        if (parts.length < 4 || !validImportFileTypes.stream().anyMatch(str -> key.toLowerCase().endsWith(str))) {
            return S3ImportMessageType.UNDEFINED;
        } else {
            if (DCP_PATTERN.matcher(key).find()) {
                return S3ImportMessageType.DCP;
            } else if (ATLAS_PATTERN.matcher(key).find() || LEGACY_ATLAS_PATTERN.matcher(key).find()) {
                return S3ImportMessageType.Atlas;
            } else if (LIST_SEGMENT_PATTERN.matcher(key).find()) {
                return S3ImportMessageType.LISTSEGMENT;
            } else if (DATA_OPERATION_PATTERN.matcher(key).find()) {
                return S3ImportMessageType.DATAOPERATION;
            } else if (ENTERPRISE_INTEGRATION_PATTERN.matcher(key).find()) {
                return S3ImportMessageType.INBOUND_CONNECTION;
            }
            else {
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
            case LISTSEGMENT:
            case DATAOPERATION:
                break;
            default:
                skip = true;
                break;
        }
        return skip;
    }

    public static List<String> getKeyPartValues(String key, S3ImportMessageType messageType, List<KeyPart> keyParts) {
        if (S3ImportMessageType.INBOUND_CONNECTION.equals(messageType)) {
            Matcher matcher = ENTERPRISE_INTEGRATION_PATTERN.matcher(key);
            boolean found = matcher.find();
            List<String> result = new ArrayList<>();
            for (KeyPart keyPart : keyParts) {
                if (found) {
                    switch (keyPart) {
                        case TENANT_ID:
                            result.add(matcher.group(1));
                            break;
                        case SOURCE_ID:
                            result.add(matcher.group(2));
                            break;
                        case FILE_NAME:
                            result.add(matcher.group(4));
                            break;
                        default:
                            result.add(StringUtils.EMPTY);
                            break;
                    }
                } else {
                    result.add(StringUtils.EMPTY);
                }
            }
            return result;
        } else {
            throw new NotImplementedException("Message type: " + messageType + " is not supported yet.");
        }
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
        } else if (S3ImportMessageType.LISTSEGMENT.equals(messageType)) {
            Matcher matcher = LIST_SEGMENT_PATTERN.matcher(key);
            if (matcher.find()) {
                String result;
                switch (keyPart) {
                    case TENANT_ID:
                        result = matcher.group(1);
                        break;
                    case SEGMENT_NAME:
                        result = matcher.group(2);
                        break;
                    case FILE_NAME:
                        result = matcher.group(3);
                        break;
                    default:
                        result = StringUtils.EMPTY;
                        break;
                }
                return result;
            } else {
                return StringUtils.EMPTY;
            }
        }
        else {
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

    public static String getSegmentIndex(S3ImportMessage message) {
        String tenantId = S3ImportMessageUtils.getKeyPart(message.getKey(), S3ImportMessageType.LISTSEGMENT,
                S3ImportMessageUtils.KeyPart.TENANT_ID);
        String segmentName = S3ImportMessageUtils.getKeyPart(message.getKey(), S3ImportMessageType.LISTSEGMENT,
                S3ImportMessageUtils.KeyPart.SEGMENT_NAME);
        return tenantId + "_" + segmentName;
    }

    public static String getDropPathFromMessage(S3ImportMessage message) {
        String key = message.getKey();
        if (StringUtils.isEmpty(key)) {
            return StringUtils.EMPTY;
        }
        return message.getBucket() + "/" + key.substring(0, key.lastIndexOf("/") + 1);
    }

    public enum KeyPart {
        PROJECT_ID, SOURCE_ID, FILE_NAME, TENANT_ID, SEGMENT_NAME
    }
}
