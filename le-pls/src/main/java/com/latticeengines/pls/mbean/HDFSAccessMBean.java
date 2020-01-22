package com.latticeengines.pls.mbean;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

@Component("plsHdfsAcMBean")
@ManagedResource(objectName = "Diagnostics:name=HDFSAccessCheck2")
public class HDFSAccessMBean {

    @Inject
    private Configuration yarnConfiguration;

    @ManagedOperation(description = "Check HDFS Accessibility")
    public String checkHDFSAccess() {
        try {
            FileSystem fs = FileSystem.get(yarnConfiguration);
            Path p = new Path("/tmp/hdfs_check");
            fs.create(p);
            fs.getFileStatus(p);
            fs.delete(p, false);
            return "[SUCCESS] HDFS is accessible to pls web server.";
        } catch (Exception e) {
            return "[FAILURE] HDFS is not accessible to pls web server. \n" + ExceptionUtils.getStackTrace(e);
        }
    }
}
