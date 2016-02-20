package com.latticeengines.dellebi.util;

import java.net.URI;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;

import cascading.flow.FlowDef;

import com.latticeengines.common.exposed.util.HdfsUtils;

public class HadoopFileSystemOperations {
    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;

    @Value("${dellebi.datahadoopinpath}")
    private String dataHadoopInPath;
    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.quotetrans}")
    private String quoteTrans;

    private static final Log log = LogFactory.getLog(HadoopFileSystemOperations.class);

    public void cleanFolder(String folderName) {
        try {
            Configuration conf = new Configuration();
            if (HdfsUtils.fileExists(conf, folderName))
                HdfsUtils.rmdir(conf, folderName);

        } catch (Exception e) {
            log.warn("Failed to delete dir!", e);
        }

    }

    public int listFileNumber(String folderName) {
        int fileNumber = 0;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            Path path = new Path(folderName);
            FileStatus[] status = fs.listStatus(path);
            for (FileStatus s : status) {
                String name = s.getPath().getName();
                if (name.endsWith(".txt")) {
                    log.info("Cascading found txt file: " + name + " and starts to process it.");
                    fileNumber++;
                }
            }

        } catch (Exception e) {
            log.warn("Failed!", e);
        }
        return fileNumber;
    }

    public boolean isExistWithTXTFile(String folderName) {

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), conf);
            Path path = new Path(folderName);
            if (fs.exists(path)) {
                FileStatus[] fileStatus = fs.listStatus(path);

                for (FileStatus status : fileStatus) {
                    if (status.getPath().getName().contains(".txt")) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to check if " + folderName + "exists or not.", e);
        }

        return false;
    }

    public static void addClasspath(FlowDef flow, String artifactVersion) {

        try {
            Configuration config = new Configuration();
            String libPath = StringUtils.isEmpty(artifactVersion) ? "/app/dellebi/lib"
                    : String.format("/app/%s/dellebi/lib/", artifactVersion);
            List<String> files = HdfsUtils.getFilesForDir(config, libPath);
            for (String file : files) {
                flow.addToClassPath(file);
            }
        } catch (Exception e) {
            log.warn("Exception retrieving library jars for this flow.", e);
        }
    }
}
