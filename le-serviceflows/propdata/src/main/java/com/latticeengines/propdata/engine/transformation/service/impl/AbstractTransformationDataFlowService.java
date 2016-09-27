package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.common.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class AbstractTransformationDataFlowService {

    protected static final String SPLIT_REGEX = "\\|";
    protected static final String HIPHEN = "-";

    @Value("${dataplatform.queue.scheme:legacy}")
    private String yarnQueueScheme;

    @Value("${propdata.collection.cascading.platform:tez}")
    private String cascadingPlatform;

    @Autowired
    protected DataTransformationService dataTransformationService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    protected DataFlowContext dataFlowContext(Source source, Map<String, Table> sources, DataFlowParameters parameters,
            String outputDir) {
        String sourceName = source.getSourceName();
        DataFlowContext ctx = new DataFlowContext();

        // TODO - anoop - enable TEZ once object mapper jar version conflict is fixed
        if ("tez".equalsIgnoreCase(getCascadingPlatform())) {
            ctx.setProperty(DataFlowProperty.ENGINE, "TEZ");
        } else {
            ctx.setProperty(DataFlowProperty.ENGINE, "MR");
        }

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
        ctx.setProperty(DataFlowProperty.HADOOPCONF, yarnConfiguration);
        ctx.setProperty(DataFlowProperty.JOBPROPERTIES, getJobProperties());
        ctx.setProperty(DataFlowProperty.ENFORCEGLOBALORDERING, false);
        return ctx;
    }

    private Properties getJobProperties() {
        Properties jobProperties = new Properties();
        jobProperties.put("mapreduce.job.reduces", "8");
        jobProperties.put("mapreduce.job.running.map.limit", "64");
        jobProperties.put("mapreduce.output.fileoutputformat.compress", "true");
        jobProperties.put("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        jobProperties.put("mapreduce.output.fileoutputformat.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");

        jobProperties.put("tez.runtime.compress", "true");
        jobProperties.put("tez.runtime.compress.codec", "org.apache.hadoop.io.compress. SnappyCodec");

        return jobProperties;
    }

    protected String getCascadingPlatform() {
        return cascadingPlatform;
    }
}
