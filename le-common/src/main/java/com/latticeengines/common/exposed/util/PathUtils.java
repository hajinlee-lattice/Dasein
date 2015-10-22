package com.latticeengines.common.exposed.util;

public class PathUtils {

    public static String stripoutProtocal(String hdfsPath) {
        return hdfsPath.replaceFirst("[^:]*://[^/]*/", "/");
    }
}
