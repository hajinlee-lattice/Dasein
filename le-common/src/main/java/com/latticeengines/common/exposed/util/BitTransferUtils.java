package com.latticeengines.common.exposed.util;

public class BitTransferUtils {

    public static String formatSize(Long fileSize) {
        if (fileSize < 1024) {
            return fileSize + "B";
        } else {
            fileSize = fileSize / 1024;
        }
        if (fileSize < 1024) {
            fileSize = fileSize * 100;
            return (fileSize / 100) + "."
                    + (fileSize % 100) + "B";
        } else {
            fileSize = fileSize / 1024;
        }
        if (fileSize < 1024) {
            fileSize = fileSize * 100;
            return (fileSize / 100) + "."
                    + (fileSize % 100) + "MB";
        } else {
            fileSize = fileSize * 100 / 1024;
            return (fileSize / 100) + "."
                    + (fileSize % 100) + "GB";
        }
    }
}
