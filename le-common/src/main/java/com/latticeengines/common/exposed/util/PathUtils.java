package com.latticeengines.common.exposed.util;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public final class PathUtils {

    protected PathUtils() {
        throw new UnsupportedOperationException();
    }

    public static String stripoutProtocol(String hdfsPath) {
        return hdfsPath.replaceFirst("[^:]*://[^/]*/", "/");
    }

    public static String toDirWithoutTrailingSlash(String path) {
        String dirPath = path;
        if (path.endsWith("/")) {
            dirPath = path.substring(0, path.lastIndexOf("/"));
        } else if (path.contains("/")) {
            String lastPart = path.substring(path.lastIndexOf("/"));
            if (lastPart.contains(".")) {
                dirPath = path.substring(0, path.lastIndexOf("/"));
            }
        }
        return dirPath;
    }

    /*-
     * Produce glob pattern with specified number of layer of child directories
     */
    public static String toNestedDirGlob(@NotNull String path, int numNestedDirs) {
        Preconditions.checkNotNull(path);
        // remove file extension
        path = toParquetOrAvroDir(path);
        StringBuilder sb = new StringBuilder(path);
        for (int i = 0; i < numNestedDirs; i++) {
            if (sb.charAt(sb.length() - 1) != '/') {
                sb.append('/');
            }
            sb.append("*");
        }
        return StringUtils.removeEnd(sb.toString(), "/");
    }

    public static String toAvroGlob(String path) {
        if (path.endsWith(".avro")) {
            return path;
        } else {
            return toParquetOrAvroDir(path) + "/*.avro";
        }
    }

    public static String toCSVGlob(String path) {
        if (path.endsWith(".csv")) {
            return path;
        } else {
            return toCSVDir(path) + "/*.csv";
        }
    }

    public static String toCSVDir(String path) {
        if (path.endsWith(".csv") || path.endsWith("/")) {
            return path.substring(0, path.lastIndexOf("/"));
        } else {
            return path;
        }
    }

    public static String toParquetGlob(String path) {
        if (path.endsWith(".parquet")) {
            return path;
        } else {
            return toParquetOrAvroDir(path) + "/*.parquet";
        }
    }

    public static String toParquetOrAvroDir(String path) {
        if (path.endsWith(".avro") || path.endsWith(".parquet") || path.endsWith("/")) {
            return path.substring(0, path.lastIndexOf("/"));
        } else {
            return path;
        }
    }

    public static String formatPath(String path) {
        if (StringUtils.isNotEmpty(path)) {
            while (path.startsWith("/")) {
                path = path.substring(1);
            }
            while (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
        }
        return path;
    }

    public static String formatKey(String s3Bucket, String path) {
        String key = formatPath(path);
        if (key.startsWith(s3Bucket)) {
            key = key.replaceFirst(s3Bucket, "");
            key = formatPath(key);
        }
        return key;
    }

    public static String getFileType(String path) {
        path = formatPath(path);
        if (path.contains(".")) {
            return path.substring(path.lastIndexOf(".") + 1);
        }
        return null;
    }

    public static String getFileNameWithoutExtension(String path) {
        path = path.substring(path.lastIndexOf('/') + 1);
        return path.trim().replaceFirst("\\.[^\\W]+$", "");
    }
}
