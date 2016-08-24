package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.service.TransformationDataFlowService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class AbstractTransformationDataFlowService implements TransformationDataFlowService {

    abstract Configuration getYarnConfiguration();

    abstract String getCascadingPlatform();

    @Value("${dataplatform.queue.scheme:legacy}")
    private String yarnQueueScheme;

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
        String translatedQueue = LedpQueueAssigner
                .overwriteQueueAssignment(LedpQueueAssigner.getPropDataQueueNameForSubmission(), yarnQueueScheme);

        ctx.setProperty(DataFlowProperty.QUEUE, translatedQueue);
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, getYarnConfiguration());
        ctx.setProperty(DataFlowProperty.JOBPROPERTIES, getJobProperties());
        return ctx;
    }

    private Properties getJobProperties() {
        Properties jobProperties = new Properties();
        jobProperties.put("mapreduce.job.reduces", "64");
        jobProperties.put("mapreduce.job.running.map.limit", "64");
        jobProperties.put("mapreduce.job.running.reduce.limit", "32");
        jobProperties.put("mapreduce.map.output.compress", "true");
        jobProperties.put("mapreduce.output.fileoutputformat.compress", "true");
        jobProperties.put("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        jobProperties.put("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        jobProperties.put("mapreduce.output.fileoutputformat.compress.codec",
                "org.apache.hadoop.io.compress.BZip2Codec");

        return jobProperties;
    }
}
