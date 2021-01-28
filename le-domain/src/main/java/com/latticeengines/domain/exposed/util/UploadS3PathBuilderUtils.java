package com.latticeengines.domain.exposed.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.PathUtils;

public final class UploadS3PathBuilderUtils {

    private static final String DROP_FOLDER = "dropfolder";
    private static final String SLASH = "/";
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd-HH-mm-ss.SSS";

    private static final String dropFolder = DROP_FOLDER + SLASH + "%s";

    private static final String projectDir = "/Projects/%s";
    private static final String sourceDir = projectDir + "/Sources/%s";
    private static final String dropDir = sourceDir + "/drop";
    private static final String uploadRoot = sourceDir + "/Uploads";
    private static final String uploadDir = uploadRoot + "/%s_%s";
    private static final String uploadRawDir = uploadDir + "/RawFile/";
    private static final String uploadImportDir = uploadDir + "/ImportResult/";
    private static final String uploadImportErrorDir = uploadImportDir + "ImportError/";
    private static final String uploadMatchDir = uploadDir + "/MatchResult/";

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
        return String.format(uploadRoot, projectId, sourceId);
    }

    public static String getDropFolder(String dropbox) {
        Preconditions.checkArgument(StringUtils.isNotBlank(dropbox));
        return String.format(dropFolder, dropbox);
    }

    public static String getUploadRawDir(String projectId, String sourceId, String displayName, String timestamp) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        Preconditions.checkArgument(StringUtils.isNotBlank(displayName) || StringUtils.isNotBlank(timestamp));
        return String.format(uploadRawDir, projectId, sourceId, PathUtils.getFileNameWithoutExtension(displayName), timestamp);
    }

    public static String getUploadImportResultDir(String projectId, String sourceId, String displayName, String timestamp) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        Preconditions.checkArgument(StringUtils.isNotBlank(displayName) || StringUtils.isNotBlank(timestamp));
        return String.format(uploadImportDir, projectId, sourceId, PathUtils.getFileNameWithoutExtension(displayName), timestamp);
    }

    public static String getUploadImportErrorResultDir(String projectId, String sourceId, String displayName, String timestamp) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        Preconditions.checkArgument(StringUtils.isNotBlank(displayName) || StringUtils.isNotBlank(timestamp));
        return String.format(uploadImportErrorDir, projectId, sourceId, PathUtils.getFileNameWithoutExtension(displayName), timestamp);
    }

    public static String getUploadMatchResultDir(String projectId, String sourceId, String displayName, String timestamp) {
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(StringUtils.isNotBlank(sourceId));
        Preconditions.checkArgument(StringUtils.isNotBlank(displayName) || StringUtils.isNotBlank(timestamp));
        return String.format(uploadMatchDir, projectId, sourceId, PathUtils.getFileNameWithoutExtension(displayName), timestamp);
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
