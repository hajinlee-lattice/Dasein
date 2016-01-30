package com.latticeengines.dataplatform.mbean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

@Component("hdfsRcMBean")
@ManagedResource(objectName = "Diagnostics:name=HDFSResourceCheck")
public class HDFSResourceMBean {

    @Autowired
    private Configuration yarnConfiguration;


    @ManagedOperation(description = "Check Resources(properties and python scripts) are deployed to HDFS")
    public String checkHDFSResource() {
        try {
            FileSystem fs = FileSystem.get(yarnConfiguration);
            Path path = new Path("/app");
            RemoteIterator<LocatedFileStatus> ri = fs.listFiles(path, true);
            StringBuilder sb = new StringBuilder();
            while (ri.hasNext()) {
                sb.append(ri.next().toString() + "\n");
            }
            return sb.toString();
        } catch (Exception e) {
            return "Failed to access Resource due to:\n" + e.getMessage();
        }
    }
}
