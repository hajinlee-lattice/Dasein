package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters.ENGINE_CONFIG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroParquetUtils;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.LivyConfigurer;
import com.latticeengines.domain.exposed.spark.LivyScalingConfig;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.monitor.util.TracingUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.spark.exposed.utils.SparkJobClzUtils;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

/**
 * S extends SparkJobConfig: params only for the spark job
 * T extends SparkXfmrConfig: extra params related to transformation pipeline
 */
public abstract class AbstractSparkTxfmr<S extends SparkJobConfig, T extends TransformerConfig>
        extends AbstractTransformer<T> {

    private static final Logger log = LoggerFactory.getLogger(AbstractSparkTxfmr.class);
    private static final ObjectMapper OM = new ObjectMapper();

    @Inject
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Inject
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    protected HdfsPathBuilder hdfsPathBuilder;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private LivySessionService livySessionService;

    @Inject
    private SparkJobService sparkJobService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${dataflowapi.spark.driver.cores}")
    private int driverCores;

    @Value("${dataflowapi.spark.driver.mem}")
    private String driverMem;

    @Value("${dataflowapi.spark.executor.cores}")
    private int executorCores;

    @Value("${dataflowapi.spark.executor.mem}")
    private String executorMem;

    @Value("${dataflowapi.spark.max.executors}")
    private int maxExecutors;

    @Value("${dataflowapi.spark.min.executors}")
    private int minExecutors;

    private int scalingMultiplier = 1; // can be overwrite by sub-class logic
    protected int partitionMultiplier = 1; // can be overwrite by sub-class logic

    protected S sparkJobConfig;

    public abstract String getName();

    protected abstract Class<? extends AbstractSparkJob<S>> getSparkJobClz();

    protected abstract Class<S> getJobConfigClz();

    protected abstract S getSparkJobConfig(String configStr);

    @Override
    protected boolean validateConfig(T config, List<String> sourceNames) {
        return true;
    }

    @SuppressWarnings("unchecked")
    protected boolean transformInternal(TransformationProgress progress, String workflowDir,
                                        TransformStep step) {
        ThreadLocal<LivySession> sessionHolder = new ThreadLocal<>();
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("startSparkJob").asChildOf(tracer.activeSpan()).start();
        try (Scope scope = tracer.activateSpan(span)) {
            sparkJobConfig = getSparkJobConfig(step.getConfig());

            List<DataUnit> sparkInput = getSparkInput(step);
            sparkJobConfig.setInput(sparkInput);
            sparkJobConfig.setWorkspace(workflowDir);

            configuration = getConfiguration(step.getConfig());
            preSparkJobProcessing(step, workflowDir, sparkJobConfig, configuration);
            span.log(Collections.singletonMap("preProcessing", JsonUtils.serialize(sparkJobConfig)));

            final Map<String, String> sparkProps = new HashMap<>();
            Map<String, String> extraProps = getSparkProps(step.getConfig());
            if (MapUtils.isNotEmpty(extraProps)) {
                sparkProps.putAll(extraProps);
            }

            final LivyScalingConfig scalingConfig = parseScalingConfig(step.getConfig());

            // log important spark configs in trace
            Map<String, String> configs = new HashMap<>();
            configs.put("transformerConfig", JsonUtils.serialize(configuration));
            configs.put("sparkJobConfig", JsonUtils.serialize(sparkJobConfig));
            configs.put("sparkInput", JsonUtils.serialize(sparkInput));
            configs.put("sparkProps", JsonUtils.serialize(sparkProps));
            span.log(configs);

            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            SparkJobResult sparkJobResult = retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("Attempt=" + (ctx.getRetryCount() + 1) + ": retry running spark job " //
                            + getSparkJobClz().getSimpleName());
                    log.warn("Previous failure:", ctx.getLastThrowable());
                }
                if (sessionHolder.get() != null) {
                    livySessionService.stopSession(sessionHolder.get());
                }
                sessionHolder.set(createLivySession(step, progress, sparkProps, scalingConfig));
                span.log(Collections.singletonMap("livySession", JsonUtils.serialize(sessionHolder.get())));
                try {
                    return sparkJobService.runJob(sessionHolder.get(), getSparkJobClz(), sparkJobConfig);
                } catch (Exception e) {
                    if (!SparkJobClzUtils.isExceptionRetryable(e)) {
                        log.error("Spark job failed with not retryable error, failing", e);
                        ctx.setExhaustedOnly();
                    }
                    throw e;
                }
            });
            span.log(Collections.singletonMap("sparkOutput", JsonUtils.serialize(sparkJobResult)));

            HdfsDataUnit output = sparkJobResult.getTargets().get(0);
            step.setCount(output.getCount());
            List<Schema> baseSchemas = getBaseSourceSchemas(step);
            step.setTargetSchema(getTargetSchema(output, sparkJobConfig, configuration, baseSchemas));

            postSparkJobProcessing(step, output.getPath(), sparkJobConfig, configuration, sparkJobResult);
            span.log(Collections.singletonMap("postProcessing", JsonUtils.serialize(sparkJobResult)));
        } catch (Exception e) {
            log.error("Failed to transform data", e);
            TracingUtils.logError(span, e, String.format("Failed to start spark txfmr step %s", step.getName()));
            return false;
        } finally {
            if (sessionHolder.get() != null) {
                livySessionService.stopSession(sessionHolder.get());
            }
            TracingUtils.finish(span);
        }
        return true;
    }

    protected void preSparkJobProcessing(TransformStep step, String workflowDir, S sparkJobConfig, T stepConfig) {
    }

    protected void postSparkJobProcessing(TransformStep step, String workflowDir, S sparkJobConfig, T stepConfig, SparkJobResult sparkJobResult) {
    }

    private Map<String, String> getSparkProps(String confJson) {
        Map<String, String> sparkProps = null;
        if (StringUtils.isNotBlank(confJson)) {
            JsonNode jsonNode = JsonUtils.deserialize(confJson, JsonNode.class);
            try {
                if (jsonNode.has(ENGINE_CONFIG)) {
                    TransformationFlowParameters.EngineConfiguration engineConfig = OM.treeToValue(
                            jsonNode.get(ENGINE_CONFIG), TransformationFlowParameters.EngineConfiguration.class);
                    sparkProps = engineConfig.getJobProperties();
                    log.info("Loaded spark properties: " + JsonUtils.serialize(sparkProps));
                }
            } catch (Exception e) {
                log.error("Failed to parse " + ENGINE_CONFIG + " from conf json.", e);
            }
        }
        return sparkProps;
    }

    private LivyScalingConfig parseScalingConfig(String confJson) {
        if (StringUtils.isNotBlank(confJson)) {
            JsonNode jsonNode = JsonUtils.deserialize(confJson, JsonNode.class);
            try {
                if (jsonNode.has(ENGINE_CONFIG)) {
                    TransformationFlowParameters.EngineConfiguration engineConfig = OM.treeToValue(
                            jsonNode.get(ENGINE_CONFIG), TransformationFlowParameters.EngineConfiguration.class);
                    if (engineConfig.getScalingMultiplier() != null) {
                        scalingMultiplier = Math.max(engineConfig.getScalingMultiplier(), 1);
                        log.info("Set scalingMultiplier to " + scalingMultiplier + " based on engine config.");
                    }
                }
            } catch (Exception e) {
                log.error("Failed to parse " + ENGINE_CONFIG + " from conf json.", e);
            }
        }
        return new LivyScalingConfig(scalingMultiplier, partitionMultiplier);
    }

    private List<DataUnit> getSparkInput(TransformStep step) {
        if (step.getBaseSources() != null && step.getBaseSources().length > 0) {
            Map<Source, List<String>> baseSourceVersionMap = setupBaseSourceVersionMap(step);
            return Arrays.stream(step.getBaseSources()) //
                    .map(src -> getSourceDataUnit(src, baseSourceVersionMap.get(src))) //
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private Map<Source, List<String>> setupBaseSourceVersionMap(TransformStep step) {
        Map<Source, List<String>> baseSourceVersionMap = new HashMap<>();
        for (int i = 0; i < step.getBaseSources().length; i++) {
            Source baseSource = step.getBaseSources()[i];
            List<String> versionList = baseSourceVersionMap.computeIfAbsent(baseSource, k -> new ArrayList<>());
            versionList.add(step.getBaseVersions().get(i));
        }
        return baseSourceVersionMap;
    }

    private DataUnit getSourceDataUnit(Source source, List<String> versions) {
        String sourceName = source.getSourceName();
        Table sourceTable;
        List<String> partitionKeys = null;
        if (source instanceof TableSource) {
            TableSource tableSource = (TableSource) source;
            sourceTable = metadataProxy.getTable(tableSource.getCustomerSpace().toString(), tableSource.getTable()
                    .getName());
            partitionKeys = tableSource.getPartitionKeys();
        } else if (versions.size() == 1) {
            sourceTable = hdfsSourceEntityMgr.getTableAtVersion(source, versions.get(0));
        } else {
            sourceTable = hdfsSourceEntityMgr.getTableAtVersions(source, versions);
        }
        List<Extract> extracts = sourceTable.getExtracts();
        int numExtracts = CollectionUtils.size(extracts);
        if (numExtracts != 1) {
            throw new UnsupportedOperationException("Source " + sourceName + " has " + numExtracts //
                    + " extracts. But only single extract tables are supported by Spark now.");
        }
        log.info("Select source {}@versions {}, partitionKeys are {}", sourceName, StringUtils.join(versions, ","),
                partitionKeys);
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            return sourceTable.partitionedToHdfsDataUnit(sourceName, partitionKeys);
        } else {
            return sourceTable.toHdfsDataUnit(sourceName);
        }
    }

    private List<Schema> getBaseSourceSchemas(TransformStep step) {
        Source[] baseSources = step.getBaseSources();
        List<String> baseSourceVersions = step.getBaseVersions();
        List<Schema> schemas = new ArrayList<>();
        for (int i = 0; i < baseSources.length; i++) {
            Source source = baseSources[i];
            String version = baseSourceVersions.get(i);
            Schema schema;
            if (source instanceof TableSource) {
                Table table = hdfsSourceEntityMgr.getTableAtVersion(source, version);
                schema = TableUtils.createSchema(AvroUtils.getAvroFriendlyString(table.getName()), table);
                log.info("Created schema from table for " + table.getName());
            } else {
                schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(source.getSourceName(), version);
                log.info("Read avsc schema for source " + source.getSourceName());
            }
            schemas.add(schema);
        }
        return schemas;
    }

    protected Schema getTargetSchema(HdfsDataUnit result, S sparkJobConfig, T configuration, List<Schema> baseSchemas) {
        if (CollectionUtils.isNotEmpty(baseSchemas)) {
            Map<String, Schema.Field> inputFields = new HashMap<>();
            baseSchemas.forEach(schema -> {
                if (schema != null) {
                    schema.getFields().forEach(field -> inputFields.putIfAbsent(field.name(), field));
                }
            });
            Schema resultSchema = AvroParquetUtils.parseAvroSchemaInDirectory(yarnConfiguration, result.getPath());
            return AvroUtils.overwriteFields(resultSchema, inputFields);
        } else {
            return null;
        }
    }

    private LivySession createLivySession(TransformStep step, TransformationProgress progress, //
                                          Map<String, String> sparkProps, LivyScalingConfig scalingConfig) {
        String creator = progress.getCreatedBy();
        String primaryJobName = creator + "~" + getName();
        String secondaryJobName = getSecondaryJobName(progress, step);
        String jobName = StringUtils.isNotBlank(secondaryJobName) //
                ? primaryJobName + "~" + secondaryJobName : primaryJobName;
        LivyConfigurer configurer = new LivyConfigurer() //
                .withDriverMem(driverMem).withDriverCores(driverCores) //
                .withExecutorMem(executorMem).withExecutorCores(executorCores) //
                .withMinExecutors(minExecutors).withMaxExecutors(maxExecutors);
        Map<String, String> sparkConf = configurer.getSparkConf(scalingConfig);
        if (MapUtils.isNotEmpty(sparkProps)) {
            sparkConf.putAll(sparkProps);
        }
        return livySessionService.startSession(jobName, configurer.getLivyConf(scalingConfig), sparkConf);
    }

    @Override
    public String outputSubDir() {
        return "Output1";
    }

    private String getSecondaryJobName(TransformationProgress progress, TransformStep step) {
        String pipelineName = progress.getPipelineName();
        if (StringUtils.isBlank(pipelineName)) {
            return progress.getSourceName();
        } else {
            return pipelineName + "~" + step.getName();
        }
    }

}
