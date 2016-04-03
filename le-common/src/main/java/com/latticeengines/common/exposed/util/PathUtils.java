package com.latticeengines.common.exposed.util;

public class PathUtils {

    public static String stripoutProtocol(String hdfsPath) {
        return hdfsPath.replaceFirst("[^:]*://[^/]*/", "/");
    }
}
