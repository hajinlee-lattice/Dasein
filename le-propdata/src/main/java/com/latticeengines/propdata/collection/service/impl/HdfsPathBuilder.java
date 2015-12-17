package com.latticeengines.propdata.collection.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.propdata.collection.source.Source;

@Component("hdfsPathBuilder")
public class HdfsPathBuilder {

    private static final String rawDataFlowType = "Raw";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_z");

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Value("${propdata.hdfs.pod.id:Default}")
    private String podId;

    Path constructSourceDir(Source source) {
        String sourceName = source.getSourceName();
        sourceName = sourceName.endsWith("/") ? sourceName.substring(0, sourceName.lastIndexOf("/")) : sourceName;
        return new Path("/Pods").append(podId).append("Services").append("PropData")
                .append("Sources").append(sourceName);
    }

    Path constructRawDataFlowDir(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append(rawDataFlowType);
    }

    Path constructRawDataFlowSnapshotDir(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append("Snapshot");
    }

    Path constructWorkFlowDir(Source source, String flowName) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append("WorkFlows").append(flowName);
    }

    Path constructSchemaDir(Source source) {
        Path baseDir = constructSourceDir(source);
        return baseDir.append("Schema");
    }

    Path constructRawDataFlowIncrementalDir(Source source, Date archiveDate) {
        Path baseDir = constructRawDataFlowDir(source);
        return baseDir.append(dateFormat.format(archiveDate));
    }

    void changeHdfsPodId(String podId) { this.podId = podId; }
}
