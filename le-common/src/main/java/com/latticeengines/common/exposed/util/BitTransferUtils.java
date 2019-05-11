package com.latticeengines.common.exposed.util;

public class BitTransferUtils {

//    public static String formatSize(Long fileSize) {
//        if (fileSize < 1024) {
//            return fileSize + "B";
//        } else {
//            fileSize = fileSize / 1024;
//        }
//        if (fileSize < 1024) {
//            fileSize = fileSize * 100;
//            return (fileSize / 100) + "."
//                    + (fileSize % 100) + "B";
//        } else {
//            fileSize = fileSize / 1024;
//        }
//        if (fileSize < 1024) {
//            fileSize = fileSize * 100;
//            return (fileSize / 100) + "."
//                    + (fileSize % 100) + "MB";
//        } else {
//            fileSize = fileSize * 100 / 1024;
//            return (fileSize / 100) + "."
//                    + (fileSize % 100) + "GB";
//        }
//    }

    public static String formatSize(long bytes) {
        int unit = 1024;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = String.valueOf(("KMGTPE").charAt(exp - 1));
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
