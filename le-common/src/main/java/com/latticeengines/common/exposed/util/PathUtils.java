package com.latticeengines.common.exposed.util;

import org.apache.commons.lang3.StringUtils;

public class PathUtils {

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

    public static String toAvroGlob(String path) {
        if (path.endsWith(".avro")) {
            return path;
        } else {
            String glob = path.endsWith("/") ? path.substring(0, path.lastIndexOf("/")) : path;
            return glob + "/*.avro";
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
}
