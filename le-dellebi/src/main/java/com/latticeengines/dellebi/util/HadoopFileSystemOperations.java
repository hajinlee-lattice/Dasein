package com.latticeengines.dellebi.util;

import java.net.URI;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.HdfsUtils;

import cascading.flow.FlowDef;

public class HadoopFileSystemOperations {

    @Value("${dellebi.datahadooprootpath}")
    private String dataHadoopRootPath;

    @Value("${dellebi.datahadoopinpath}")
    private String dataHadoopInPath;
    @Value("${dellebi.datahadoopworkingpath}")
    private String dataHadoopWorkingPath;

    @Value("${dellebi.quotetrans}")
    private String quoteTrans;

    @Inject
    private Configuration yarnConfiguration;

    private static final Logger log = LoggerFactory.getLogger(HadoopFileSystemOperations.class);

    public void cleanFolder(String folderName) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, folderName))
                HdfsUtils.rmdir(yarnConfiguration, folderName);

        } catch (Exception e) {
            log.warn("Failed to delete dir!");
        }

    }

    public int listFileNumber(String folderName) {
        int fileNumber = 0;
        try {
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), yarnConfiguration);
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
            log.warn("Failed!");
        }
        return fileNumber;
    }

    public boolean isExistWithTXTFile(String folderName) {

        try {
            FileSystem fs = FileSystem.get(URI.create(dataHadoopRootPath), yarnConfiguration);
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
            log.warn("Failed to check if " + folderName + "exists or not.");
        }

        return false;
    }

    public static void addClasspath(Configuration yarnConfiguration, FlowDef flow, String artifactVersion) {

        try {
            String libPath = StringUtils.isEmpty(artifactVersion) ? "/app/dellebi/lib"
                    : String.format("/app/%s/dellebi/lib/", artifactVersion);
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, libPath);
            for (String file : files) {
                flow.addToClassPath(file);
            }
        } catch (Exception e) {
            log.warn("Exception retrieving library jars for this flow.");
        }
    }
}
