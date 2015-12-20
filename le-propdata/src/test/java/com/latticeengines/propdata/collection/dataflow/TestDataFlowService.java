package com.latticeengines.propdata.collection.dataflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.propdata.collection.service.impl.CollectionDataFlowServiceImpl;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("testDataFlowService")
public class TestDataFlowService extends CollectionDataFlowServiceImpl {

    public void executeFunctionalDataflow(String sourcePath, String outputDir, String dataflowBean) {
        executeFunctionalDataflow(sourcePath, outputDir, dataflowBean, null);
    }

    public void executeFunctionalDataflow(String sourcePath, String outputDir, String dataflowBean, String queue) {
        String flowName = "TestingDataFlow";
        Map<String, String> sources = new HashMap<>();
        sources.put("Source", sourcePath);
        DataFlowContext ctx = commonContext(queue);
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("TARGETPATH", outputDir);
        ctx.setProperty("FLOWNAME", flowName);
        dataTransformationService.executeNamedTransformation(ctx, dataflowBean);
    }

    public void executeJoinDataflow(String lhsPath, String rhsPath, String outputDir, String dataflowBean) {
        executeJoinDataflow(lhsPath, rhsPath, outputDir, dataflowBean, null);
    }

    public void executeJoinDataflow(String lhsPath, String rhsPath, String outputDir, String dataflowBean, String queue) {
        String flowName = "TestingJoinDataFlow";
        Map<String, String> sources = new HashMap<>();
        sources.put("Source1", lhsPath);
        sources.put("Source2", rhsPath);
        DataFlowContext ctx = commonContext(queue);
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("TARGETPATH", outputDir);
        ctx.setProperty("FLOWNAME", flowName);
        dataTransformationService.executeNamedTransformation(ctx, dataflowBean);
    }

    private DataFlowContext commonContext(String queue) {
        String sourceName = "Test";
        DataFlowContext ctx = new DataFlowContext();
        if ("mr".equalsIgnoreCase(cascadingPlatform)) {
            ctx.setProperty("ENGINE", "MR");
        } else {
            ctx.setProperty("ENGINE", "TEZ");
        }
        ctx.setProperty("CUSTOMER", sourceName);
        ctx.setProperty("RECORDNAME", sourceName);
        ctx.setProperty("TARGETTABLENAME", sourceName);

        if (StringUtils.isEmpty(queue)) {
            queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        }

        ctx.setProperty("QUEUE", queue);
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("JOBPROPERTIES", getJobProperties());
        return ctx;
    }

}
