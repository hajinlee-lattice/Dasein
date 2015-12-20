package com.latticeengines.propdata.collection.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.CollectionDataFlowService;
import com.latticeengines.propdata.collection.source.Source;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("collectionDataFlowService")
public class CollectionDataFlowServiceImpl implements CollectionDataFlowService {

    @Autowired
    protected DataTransformationService dataTransformationService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Value("${propdata.collection.use.default.job.properties:true}")
    private boolean useDefaultProperties;

    @Value("${propdata.collection.mapred.reduce.tasks:8}")
    private int reduceTasks;

    @Value("${propdata.collection.cascading.platform:tez}")
    protected String cascadingPlatform;

    @Override
    public void executeMergeRawSnapshotData(Source source, String mergeDataFlowQualifier, String uid) {
        String flowName = CollectionDataFlowKeys.MERGE_RAW_SNAPSHOT_FLOW;

        Map<String, String> sources = new HashMap<>();

        String rawDir = hdfsPathBuilder.constructRawDir(source).toString();
        try {
            for (String dir : HdfsUtils.getFilesForDir(yarnConfiguration, rawDir)) {
                if (HdfsUtils.isDirectory(yarnConfiguration, dir)) {
                    dir = dir.substring(dir.lastIndexOf("/") + 1);
                    sources.put(dir, rawDir + "/" + dir + "/*.avro");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get all incremental raw data dirs for " + source.getSourceName());
        }

        String outputDir = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();

        DataFlowContext ctx = commonContext(source, sources);
        ctx.setProperty("TARGETPATH", outputDir);
        ctx.setProperty("FLOWNAME", source.getSourceName() + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, mergeDataFlowQualifier);
    }


    @Override
    public void executePivotSnapshotData(Source source, String snapshotDir, String pivotDataFlowQualifier, String uid) {
        String flowName = CollectionDataFlowKeys.PIVOT_SNAPSHOT_FLOW;

        String targetPath = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();

        Map<String, String> sources = new HashMap<>();
        sources.put(CollectionDataFlowKeys.SNAPSHOT_SOURCE, snapshotDir + "/*.avro");

        DataFlowContext ctx = commonContext(source, sources);
        ctx.setProperty("TARGETPATH", targetPath);
        ctx.setProperty("FLOWNAME", source.getSourceName() + "-" + flowName);
        dataTransformationService.executeNamedTransformation(ctx, pivotDataFlowQualifier);
    }

    @Override
    public void executeJoin(String lhsPath, String rhsPath, String outputDir, String dataflowBean) {
        String flowName = "TestingJoinDataFlow";
        Map<String, String> sources = new HashMap<>();
        sources.put("Source1", lhsPath);
        sources.put("Source2", rhsPath);
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("ENGINE", "TEZ");
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("CUSTOMER", "PropDataMatchTest");
        ctx.setProperty("RECORDNAME", "PropDataMatchTest");
        ctx.setProperty("TARGETTABLENAME", "PropDataMatchTest");

        ctx.setProperty("QUEUE", LedpQueueAssigner.getPropDataQueueNameForSubmission());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("JOBPROPERTIES", getJobProperties());

        ctx.setProperty("TARGETPATH", outputDir);
        ctx.setProperty("FLOWNAME", flowName);
        dataTransformationService.executeNamedTransformation(ctx, dataflowBean);
    }

    private DataFlowContext commonContext(Source source, Map<String, String> sources) {
        String sourceName = source.getSourceName();
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

    protected Properties getJobProperties() {
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
