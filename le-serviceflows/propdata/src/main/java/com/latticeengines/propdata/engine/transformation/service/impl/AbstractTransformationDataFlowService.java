package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.Map;
import java.util.Properties;

import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.service.TransformationDataFlowService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class AbstractTransformationDataFlowService implements TransformationDataFlowService {

    abstract Configuration getYarnConfiguration();

    abstract String getCascadingPlatform();

    protected DataFlowContext dataFlowContext(Source source, Map<String, Table> sources, DataFlowParameters parameters,
            String outputDir) {
        String sourceName = source.getSourceName();
        DataFlowContext ctx = new DataFlowContext();
        // TODO - anoop - enable TEZ once object mapper jar version conflict is
        // fixed
        // if ("mr".equalsIgnoreCase(getCascadingPlatform())) {
        // ctx.setProperty("ENGINE", "MR");
        // } else {
        // ctx.setProperty("ENGINE", "TEZ");
        // }
        ctx.setProperty(DataFlowProperty.ENGINE, "MR");

        ctx.setProperty(DataFlowProperty.PARAMETERS, parameters);
        ctx.setProperty(DataFlowProperty.SOURCETABLES, sources);
        ctx.setProperty(DataFlowProperty.CUSTOMER, sourceName);
        ctx.setProperty(DataFlowProperty.RECORDNAME, sourceName);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, sourceName);
        ctx.setProperty(DataFlowProperty.TARGETPATH, outputDir);

        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getPropDataQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, getYarnConfiguration());
        ctx.setProperty(DataFlowProperty.JOBPROPERTIES, new Properties());
        return ctx;
    }
}
