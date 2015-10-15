package com.latticeengines.propdata.collection.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.Path;

@Component("hdfsPathBuilder")
public class HdfsPathBuilder {

    private static final String rawDataFlowType = "Raw";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    @Value("${propdata.hdfs.pod.id:Default}")
    private String podId;

    Path constructSourceDir(String sourceName) {
        sourceName = sourceName.endsWith("/") ? sourceName.substring(0, sourceName.lastIndexOf("/")) : sourceName;
        return new Path("/Pods").append(podId).append("Services").append("PropData")
                .append("Sources").append(sourceName);
    }

    Path constructRawDataFlowDir(String sourceName) {
        Path baseDir = constructSourceDir(sourceName);
        return baseDir.append("DataFlows").append(rawDataFlowType);
    }

    Path constructRawDataFlowSnapshotDir(String sourceName) {
        Path baseDir = constructSourceDir(sourceName);
        return baseDir.append("DataFlows").append("Snapshot");
    }

    Path constructRawDataFlowIncrementalDir(String sourceName, Date startDate, Date endDate) {
        Path baseDir = constructRawDataFlowDir(sourceName);
        return baseDir.append(dateFormat.format(startDate) + "-" + dateFormat.format(endDate));
    }

    Path constructWorkFlowDir(String sourceName, String flowName) {
        Path baseDir = constructSourceDir(sourceName);
        return baseDir.append("WorkFlows").append(flowName);
    }

    Path constructSchemaDir(String sourceName) {
        Path baseDir = constructSourceDir(sourceName);
        return baseDir.append("DataFlows").append("Schema");
    }

    void changeHdfsPodId(String podId) { this.podId = podId; }
}
