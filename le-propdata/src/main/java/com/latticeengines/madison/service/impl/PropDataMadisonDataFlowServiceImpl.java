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
    public void execute(String flowName, List<String> sourcePaths, String targetPath) {

        Map<String, String> sources = new HashMap<>();
        sources.put("MadisonLogic0", sourcePaths.get(0));
        if (sourcePaths.size() > 1) {
            sources.put("MadisonLogic1", sourcePaths.get(1));
        }

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("CUSTOMER", "MadisonLogic");
        ctx.setProperty("TARGETPATH", targetPath);
        ctx.setProperty("QUEUE", "Priority0.MapReduce.0");
        ctx.setProperty("FLOWNAME", flowName);
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", new Configuration());
//        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        dataTransformationService.executeNamedTransformation(ctx, "madisonDataFlowBuilder");
    }
}
