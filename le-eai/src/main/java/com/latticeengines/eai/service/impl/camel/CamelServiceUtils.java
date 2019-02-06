package com.latticeengines.eai.service.impl.camel;

import java.io.File;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.HdfsUtils;

public class CamelServiceUtils {

    static Long checkHdfsFileSize(Configuration yarnConfiguration, String hdfsDir, String fileName, String openSuffix) {
        try {
            String tmpFileName = fileName + "." + openSuffix;
            hdfsDir = cleanDirPath(hdfsDir);
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir + fileName)) {
                return HdfsUtils.getFileSize(yarnConfiguration, hdfsDir + fileName);
            } else if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir + tmpFileName)) {
                return HdfsUtils.getFileSize(yarnConfiguration, hdfsDir + tmpFileName);
            } else {
                return 0L;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to check hdfs file size: " + hdfsDir + "/" + fileName, e);
        }
    }

    static Long checkLocalFileSize(String dir, String filename, String suffix) {
        String fileName = filename + "." + suffix;
        File tempFile = new File(dir + File.separator + fileName);
        if (tempFile.exists()) {
            return tempFile.length();
        } else {
            throw new RuntimeException("Could not find local temp file: " + fileName);
        }
    }

    private static String cleanDirPath(String hdfsDir) {
        while (hdfsDir.endsWith("/")) {
            hdfsDir = hdfsDir.substring(0, hdfsDir.lastIndexOf("/"));
        }
        if (!hdfsDir.startsWith("/")) {
            hdfsDir = "/" + hdfsDir;
        }
        return hdfsDir + "/";
    }
}
