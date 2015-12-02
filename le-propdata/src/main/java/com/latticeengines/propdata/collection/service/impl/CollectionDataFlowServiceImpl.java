package com.latticeengines.propdata.collection.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.CollectionDataFlowService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class CollectionDataFlowServiceImpl implements CollectionDataFlowService {

    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Value("${propdata.collection.use.default.job.properties:true}")
    private boolean useDefaultProperties;

    @Value("${propdata.collection.mapred.reduce.tasks:8}")
    private int reduceTasks;

    @Value("${propdata.collection.cascading.platform:tez}")
    private String cascadingPlatform;

    @Override
    public void executeMergeRawSnapshotData(String sourceName, String rawDir, String mergeDataFlowQualifier) {
        String flowName = CollectionDataFlowKeys.MERGE_RAW_SNAPSHOT_FLOW;

        String targetPath = hdfsPathBuilder.constructWorkFlowDir(sourceName, flowName).toString();
        String snapshotPath = hdfsPathBuilder.constructRawDataFlowSnapshotDir(sourceName).toString();

        Map<String, String> sources = new HashMap<>();
        sources.put(CollectionDataFlowKeys.RAW_AVRO_SOURCE, rawDir + "/*.avro");
        sources.put(CollectionDataFlowKeys.DEST_SNAPSHOT_SOURCE, snapshotPath + "/*.avro");

        DataFlowContext ctx = commonContext(sourceName, sources);
        ctx.setProperty("TARGETPATH", targetPath + "/Output");
        ctx.setProperty("FLOWNAME", sourceName + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, mergeDataFlowQualifier);
    }


    @Override
    public void executePivotSnapshotData(String sourceName, String snapshotDir, String pivotDataFlowQualifier) {
        String flowName = CollectionDataFlowKeys.PIVOT_SNAPSHOT_FLOW;

        String targetPath = hdfsPathBuilder.constructWorkFlowDir(sourceName, flowName).toString();
        String snapshotPath = snapshotDir;

        Map<String, String> sources = new HashMap<>();
        sources.put(CollectionDataFlowKeys.DEST_SNAPSHOT_SOURCE, snapshotPath + "/*.avro");

        DataFlowContext ctx = commonContext(sourceName, sources);
        ctx.setProperty("TARGETPATH", targetPath + "/Output");
        ctx.setProperty("FLOWNAME", sourceName + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, pivotDataFlowQualifier);
    }

    private DataFlowContext commonContext(String sourceName, Map<String, String> sources) {
        DataFlowContext ctx = new DataFlowContext();
        if ("mr".equalsIgnoreCase(cascadingPlatform)) {
            ctx.setProperty("ENGINE", "MR");
        } else {
            ctx.setProperty("ENGINE", "TEZ");
        }
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("CUSTOMER", sourceName);
        ctx.setProperty("RECORDNAME", sourceName);
        ctx.setProperty("TARGETTABLENAME", sourceName);

        ctx.setProperty("QUEUE", LedpQueueAssigner.getPropDataQueueNameForSubmission());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("JOBPROPERTIES", getJobProperties());
        return ctx;
    }

    private Properties getJobProperties() {
        Properties jobProperties = new Properties();
        if (!useDefaultProperties) {
            jobProperties.put("mapred.reduce.tasks", String.valueOf(reduceTasks));
            jobProperties.put("mapred.tasktracker.map.tasks.maximum", "8");
            jobProperties.put("mapred.tasktracker.reduce.tasks.maximum", "8");
            jobProperties.put("mapred.compress.map.output", "true");
            jobProperties.put("mapred.output.compression.type", "BLOCK");
            jobProperties.put("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        }
        jobProperties.put("mapred.mapper.new-api", "false");
        return jobProperties;
    }

}
