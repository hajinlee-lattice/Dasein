package com.latticeengines.common.exposed.util;

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
}
