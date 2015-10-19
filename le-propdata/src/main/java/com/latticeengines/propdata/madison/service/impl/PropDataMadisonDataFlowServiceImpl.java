package com.latticeengines.propdata.madison.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.propdata.madison.service.PropDataMadisonDataFlowService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class PropDataMadisonDataFlowServiceImpl implements PropDataMadisonDataFlowService {

    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Value("${propdata.madison.use.default.job.properties}")
    private boolean useDefaultProperties;

    @Override
    public void execute(String flowName, List<String> sourcePaths, String targetPath, String targetSchemaPath) {

        executeAggregation(flowName, sourcePaths, targetPath, targetSchemaPath);

        executeGroupAndExpand(flowName, sourcePaths, targetPath, targetSchemaPath);
    }

    private void executeAggregation(String flowName, List<String> sourcePaths, String targetPath,
            String targetSchemaPath) {

        Map<String, String> sources = new HashMap<>();
        sources.put("MadisonLogic0", sourcePaths.get(0) + "/*.avro");
        if (sourcePaths.size() > 1) {
            sources.put("MadisonLogic1", sourcePaths.get(1) + "/1/*.avro");
        }

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("CUSTOMER", "MadisonLogic");
        ctx.setProperty("TARGETPATH", targetPath + "/1");
        ctx.setProperty("TARGETTABLENAME", "MadisonLogic");

        ctx.setProperty("QUEUE", LedpQueueAssigner.getPropDataQueueNameForSubmission());
        ctx.setProperty("FLOWNAME", flowName + "-Aggregation");
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("JOBPROPERTIES", getJobProperties());
        dataTransformationService.executeNamedTransformation(ctx, "madisonDataFlowAggregationBuilder");
    }

    private void executeGroupAndExpand(String flowName, List<String> sourcePaths, String targetPath,
            String targetSchemaPath) {
        Map<String, String> sources = new HashMap<>();
        sources.put("MadisonLogic0", targetPath + "/1/*.avro");

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("CUSTOMER", "MadisonLogic");
        ctx.setProperty("TARGETPATH", targetPath + "/output");
        ctx.setProperty("TARGETSCHEMAPATH", targetSchemaPath + "/*.avro");
        ctx.setProperty("TARGETTABLENAME", "MadisonLogic");

        ctx.setProperty("QUEUE", LedpQueueAssigner.getPropDataQueueNameForSubmission());
        ctx.setProperty("FLOWNAME", flowName + "-GroupAndExpand");
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("JOBPROPERTIES", getJobProperties());
        dataTransformationService.executeNamedTransformation(ctx, "madisonDataFlowGroupAndExpandBuilder");
    }

    private Properties getJobProperties() {
        Properties jobProperties = new Properties();
        if (useDefaultProperties) {
            return jobProperties;
        }
        jobProperties.put("mapred.reduce.tasks", "72");
        jobProperties.put("mapred.tasktracker.map.tasks.maximum", "8");
        jobProperties.put("mapred.tasktracker.reduce.tasks.maximum", "8");
        jobProperties.put("mapred.compress.map.output", "true");
        jobProperties.put("mapred.output.compression.type", "BLOCK");
        jobProperties.put("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");

        return jobProperties;
    }
}
