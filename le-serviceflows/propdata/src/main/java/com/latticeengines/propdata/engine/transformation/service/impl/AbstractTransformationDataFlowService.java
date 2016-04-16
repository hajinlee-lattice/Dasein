package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.service.TransformationDataFlowService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class AbstractTransformationDataFlowService implements TransformationDataFlowService {

    protected DataFlowContext dataFlowContext(Source source, Map<String, Table> sources, DataFlowParameters parameters,
            String outputDir) {
        String sourceName = source.getSourceName();
        DataFlowContext ctx = new DataFlowContext();
        if ("mr".equalsIgnoreCase(getCascadingPlatform())) {
            ctx.setProperty("ENGINE", "MR");
        } else {
            ctx.setProperty("ENGINE", "TEZ");
        }

        ctx.setProperty("PARAMETERS", parameters);
        ctx.setProperty("SOURCETABLES", sources);
        ctx.setProperty("CUSTOMER", sourceName);
        ctx.setProperty("RECORDNAME", sourceName);
        ctx.setProperty("TARGETTABLENAME", sourceName);
        ctx.setProperty("TARGETPATH", outputDir);

        ctx.setProperty("QUEUE", LedpQueueAssigner.getPropDataQueueNameForSubmission());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", getYarnConfiguration());
        ctx.setProperty("JOBPROPERTIES", new Properties());
        return ctx;
    }

    abstract Configuration getYarnConfiguration();

    abstract String getCascadingPlatform();

}
