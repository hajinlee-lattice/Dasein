package com.latticeengines.madison.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.madison.service.PropDataMadisonDataFlowService;

@Component
public class PropDataMadisonDataFlowServiceImpl implements PropDataMadisonDataFlowService {

    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    protected Configuration yarnConfiguration;

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

        ctx.setProperty("QUEUE", "Priority0.MapReduce.0");
        ctx.setProperty("FLOWNAME", flowName + "-Aggregation");
        ctx.setProperty("CHECKPOINT", false);
//        ctx.setProperty("HADOOPCONF", new Configuration());
         ctx.setProperty("HADOOPCONF", yarnConfiguration);
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

        ctx.setProperty("QUEUE", "Priority0.MapReduce.0");
        ctx.setProperty("FLOWNAME", flowName + "-GroupAndExpand");
        ctx.setProperty("CHECKPOINT", false);
//        ctx.setProperty("HADOOPCONF", new Configuration());
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        dataTransformationService.executeNamedTransformation(ctx, "madisonDataFlowGroupAndExpandBuilder");
    }
}
