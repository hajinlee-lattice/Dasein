package com.latticeengines.dataplatform.mbean;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

@Component("hdfsAcMBean")
@ManagedResource(objectName = "Diagnostics:name=HDFSAccessCheck")
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
            return "HDFS is accessible to dataplatform.";
        } catch (Exception e) {
            return "Failed to access HDFS from dataplatform due to: " + e.getMessage();
        }
    }
}
