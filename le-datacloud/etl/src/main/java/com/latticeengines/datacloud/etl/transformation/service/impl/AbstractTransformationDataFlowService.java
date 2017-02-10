package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import org.springframework.context.ApplicationContext;

public abstract class AbstractTransformationDataFlowService {

    protected static final String SPLIT_REGEX = "\\|";
    protected static final String HIPHEN = "-";

    @Value("${dataplatform.queue.scheme:legacy}")
    private String yarnQueueScheme;

    @Value("${datacloud.etl.cascading.platform}")
    private String cascadingPlatform;

    @Value("${datacloud.etl.cascading.partitions}")
    protected Integer cascadingPartitions;

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

    @Autowired
    private ApplicationContext appCtx;

    protected DataFlowContext dataFlowContext(Source source, Map<String, Table> sources, DataFlowParameters parameters,
                                              String outputDir) {
        String sourceName = source.getSourceName();
        DataFlowContext ctx = new DataFlowContext();

        ctx.setProperty(DataFlowProperty.ENGINE, getCascadingPlatform().toUpperCase());
        ctx.setProperty(DataFlowProperty.PARAMETERS, parameters);
        ctx.setProperty(DataFlowProperty.SOURCETABLES, sources);
        ctx.setProperty(DataFlowProperty.CUSTOMER, sourceName);
        ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + HIPHEN + CollectionDataFlowKeys.TRANSFORM_FLOW);
        ctx.setProperty(DataFlowProperty.RECORDNAME, sourceName);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, sourceName);
        ctx.setProperty(DataFlowProperty.TARGETPATH, outputDir);
        String translatedQueue = LedpQueueAssigner
                .overwriteQueueAssignment(LedpQueueAssigner.getPropDataQueueNameForSubmission(), yarnQueueScheme);
        ctx.setProperty(DataFlowProperty.QUEUE, translatedQueue);
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, yarnConfiguration);
        ctx.setProperty(DataFlowProperty.PARTITIONS, cascadingPartitions);
        ctx.setProperty(DataFlowProperty.JOBPROPERTIES, getJobProperties());
        ctx.setProperty(DataFlowProperty.ENFORCEGLOBALORDERING, false);
        ctx.setProperty(DataFlowProperty.APPCTX, appCtx);

        return ctx;
    }

    private Properties getJobProperties() {
        Properties jobProperties = new Properties();
        jobProperties.put("mapreduce.job.reduces", String.valueOf(Math.min(cascadingPartitions, 16)));
        jobProperties.put("mapreduce.job.running.map.limit", "64");
        jobProperties.put("mapreduce.output.fileoutputformat.compress", "true");
        jobProperties.put("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        jobProperties.put("mapreduce.output.fileoutputformat.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");
        jobProperties.put("tez.runtime.compress", "true");
        jobProperties.put("tez.runtime.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        return jobProperties;
    }

    protected String getCascadingPlatform() {
        return cascadingPlatform;
    }
}
