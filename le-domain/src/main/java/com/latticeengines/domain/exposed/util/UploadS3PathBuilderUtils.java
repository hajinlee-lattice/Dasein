package com.latticeengines.domain.exposed.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

public final class UploadS3PathBuilderUtils {

    private static final String DROP_FOLDER = "dropfolder";
    private static final String SLASH = "/";
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd-HH-mm-ss.SSS";

    private static String dropFolder = DROP_FOLDER + SLASH + "%s";

    private static String projectDir = "/Projects/%s";
    private static String sourceDir = projectDir + "/Source/%s";
    private static String dropDir = sourceDir + "/drop";
    private static String uploadDir = sourceDir + "/upload";
    private static String uploadRawDir = uploadDir + "/%s/RawFile/";
    private static String uploadImportDir = uploadDir + "/%s/ImportResult/";
    private static String uploadImportErrorDir = uploadImportDir + "ImportError/";
    private static String uploadMatchDir = uploadDir + "/%s/MatchResult/";

    protected UploadS3PathBuilderUtils() {
        throw new UnsupportedOperationException();
    }

    public static String getTimestampString() {
        Date now = new Date();
        return new SimpleDateFormat(TIMESTAMP_FORMAT).format(now);
    }

    public static String getDropRoot(String projectId, String sourceId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        return String.format(dropDir, projectId, sourceId);
    }

    public static String getUploadRoot(String projectId, String sourceId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        return String.format(uploadDir, projectId, sourceId);
    }

    public static String getDropFolder(String dropbox) {
        Preconditions.checkArgument(StringUtils.isNotBlank(dropbox));
        return String.format(dropFolder, dropbox);
    }

    public static String getUploadRawDir(String projectId, String sourceId, String timestamp) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        Preconditions.checkArgument(StringUtils.isNotBlank(timestamp));
        return String.format(uploadRawDir, projectId, sourceId, timestamp);
    }

    public static String getUploadImportResultDir(String projectId, String sourceId, String timestamp) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        Preconditions.checkArgument(StringUtils.isNotBlank(timestamp));
        return String.format(uploadImportDir, projectId, sourceId, timestamp);
    }

    public static String getUploadImportErrorResultDir(String projectId, String sourceId, String timestamp) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        Preconditions.checkArgument(StringUtils.isNotBlank(timestamp));
        return String.format(uploadImportErrorDir, projectId, sourceId, timestamp);
    }

    public static String getUploadMatchResultDir(String projectId, String sourceId, String timestamp) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        Preconditions.checkArgument(StringUtils.isNotBlank(timestamp));
        return String.format(uploadMatchDir, projectId, sourceId, timestamp);
    }

    public static String combinePath(boolean withHeadingSlash, boolean withTailingSlash, String... pathParts) {
        for (String path : pathParts) {
            Preconditions.checkArgument(StringUtils.isNotBlank(path));
        }
        StringBuilder sb = new StringBuilder();
        if (withHeadingSlash) {
            sb.append(SLASH);
        }
        for (String path : pathParts) {
            sb.append(trimSlash(path));
            sb.append(SLASH);
        }
        if (!withTailingSlash) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    private static String trimSlash(String part) {
        part = part.startsWith(SLASH) ? part.substring(1) : part;
        part = part.endsWith(SLASH) ? part.substring(0, part.length() - 1) : part;
        return part;
    }
}
