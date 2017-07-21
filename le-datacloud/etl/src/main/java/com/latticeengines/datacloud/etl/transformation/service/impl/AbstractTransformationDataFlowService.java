package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.flink.FlinkConstants;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class AbstractTransformationDataFlowService {

    protected static final String SPLIT_REGEX = "\\|";
    protected static final String HIPHEN = "-";

    @Value("${dataplatform.queue.scheme:legacy}")
    private String yarnQueueScheme;

    @Value("${datacloud.etl.cascading.platform}")
    private String cascadingPlatform;

    @Value("${datacloud.etl.cascading.partitions}")
    protected Integer cascadingPartitions;

    @Value("${datacloud.etl.cascading.tez.task.mem.gb}")
    private int taskMem;

    @Value("${datacloud.etl.cascading.tez.task.mem.vcores}")
    private int taskVcores;

    @Value("${datacloud.etl.cascading.flink.mode}")
    private String flinkMode;

    @Value("${datacloud.etl.cascading.flink.containers}")
    private int flinkContainers;

    @Value("${datacloud.etl.cascading.flink.jm.mem.gb}")
    private int flinkJmMem;

    @Value("${datacloud.etl.cascading.flink.tm.mem.gb}")
    private int flinkTmMem;

    @Value("${datacloud.etl.cascading.flink.tm.slots}")
    private int flinkTmSlots;

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
        String engine = getCascadingEngine(parameters);

        ctx.setProperty(DataFlowProperty.ENGINE, engine.toUpperCase());
        ctx.setProperty(DataFlowProperty.PARAMETERS, parameters);
        ctx.setProperty(DataFlowProperty.SOURCETABLES, sources);
        ctx.setProperty(DataFlowProperty.CUSTOMER, sourceName);
        ctx.setProperty(DataFlowProperty.FLOWNAME,
                source.getSourceName() + HIPHEN + CollectionDataFlowKeys.TRANSFORM_FLOW);
        ctx.setProperty(DataFlowProperty.RECORDNAME, sourceName);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, sourceName);
        ctx.setProperty(DataFlowProperty.TARGETPATH, outputDir);
        String translatedQueue = LedpQueueAssigner
                .overwriteQueueAssignment(LedpQueueAssigner.getPropDataQueueNameForSubmission(), yarnQueueScheme);
        ctx.setProperty(DataFlowProperty.QUEUE, translatedQueue);
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, yarnConfiguration);
        ctx.setProperty(DataFlowProperty.ENFORCEGLOBALORDERING, false);
        ctx.setProperty(DataFlowProperty.FLINKMODE, flinkMode);
        ctx.setProperty(DataFlowProperty.APPCTX, appCtx);

        // partitions
        ctx.setProperty(DataFlowProperty.PARTITIONS, cascadingPartitions);
        if (parameters instanceof TransformationFlowParameters) {
            TransformationFlowParameters tfParameters = (TransformationFlowParameters) parameters;
            if (tfParameters.getEngineConfiguration() != null && tfParameters.getEngineConfiguration().getPartitions() != null) {
                Integer configuredPartitions = tfParameters.getEngineConfiguration().getPartitions();
                ctx.setProperty(DataFlowProperty.PARTITIONS, configuredPartitions);
            }
        }

        // job properties
        Properties jobProps = getJobProperties();
        org.apache.flink.configuration.Configuration flinkConf = getFlinkConf();
        if (parameters instanceof TransformationFlowParameters) {
            TransformationFlowParameters tfParameters = (TransformationFlowParameters) parameters;
            if (tfParameters.getEngineConfiguration() != null) {
                overwriteJobProperties(jobProps, flinkConf, tfParameters.getEngineConfiguration().getJobProperties());
            }
        }
        ctx.setProperty(DataFlowProperty.JOBPROPERTIES, jobProps);
        ctx.setProperty(DataFlowProperty.FLINKCONF, flinkConf);

        return ctx;
    }

    private String getCascadingEngine(DataFlowParameters parameters) {
        String engine = null;
        if (parameters instanceof TransformationFlowParameters) {
            TransformationFlowParameters tfParameters = (TransformationFlowParameters) parameters;
            if (tfParameters.getEngineConfiguration() != null) {
                engine = tfParameters.getEngineConfiguration().getEngine();
            }
        }
        if (StringUtils.isBlank(engine)) {
            engine = cascadingPlatform;
            if (parameters instanceof TransformationFlowParameters) {
                TransformationFlowParameters tfParameters = (TransformationFlowParameters) parameters;
                if (tfParameters.getEngineConfiguration() != null) {
                    tfParameters.getEngineConfiguration().setEngine(engine);
                }
            }
        }
        return engine;
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
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(taskMem * 1024));
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(taskVcores));
        return jobProperties;
    }

    private org.apache.flink.configuration.Configuration getFlinkConf() {
        org.apache.flink.configuration.Configuration flinkConf = new org.apache.flink.configuration.Configuration();
        flinkConf.setString(FlinkConstants.JM_HEAP_CONF, String.valueOf(flinkJmMem * 1024));
        flinkConf.setString(FlinkConstants.TM_HEAP_CONF, String.valueOf(flinkTmMem * 1024));
        flinkConf.setString(FlinkConstants.TM_SLOTS, String.valueOf(flinkTmSlots));
        flinkConf.setString(FlinkConstants.NUM_CONTAINERS, String.valueOf(flinkContainers));
        return flinkConf;
    }

    private void overwriteJobProperties(Properties jobProps, org.apache.flink.configuration.Configuration flinkConf,
            Map<String, String> properties) {
        if (properties != null) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                jobProps.put(entry.getKey(), entry.getValue());
                flinkConf.setString(entry.getKey(), entry.getValue());
            }
        }
    }

}
