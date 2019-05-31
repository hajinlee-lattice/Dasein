package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters.ENGINE_CONFIG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
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
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;

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

    @Inject
    private EMRCacheService emrCacheService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${dataflowapi.spark.driver.cores}")
    private String driverCores;

    @Value("${dataflowapi.spark.driver.mem}")
    private String driverMem;

    @Value("${dataflowapi.spark.executor.cores}")
    private String executorCores;

    @Value("${dataflowapi.spark.executor.mem}")
    private String executorMem;

    @Value("${dataflowapi.spark.max.executors}")
    private String maxExecutors;

    @Value("${dataflowapi.spark.min.executors}")
    private String minExecutors;

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
        LivySession session = null;
        try {
            sparkJobConfig = getSparkJobConfig(step.getConfig());

            List<DataUnit> sparkInput = getSparkInput(step);
            sparkJobConfig.setInput(sparkInput);
            sparkJobConfig.setWorkspace(workflowDir);

            configuration = getConfiguration(step.getConfig());
            modifySparkJobConfig(sparkJobConfig, configuration);

            Map<String, String> sparkProps = getSparkProps(step.getConfig());
            if (sparkProps == null) {
                sparkProps = new HashMap<>();
            }
            session = createLivySession(step, progress, sparkProps);

            SparkJobResult sparkJobResult = sparkJobService.runJob(session, getSparkJobClz(), sparkJobConfig);
            HdfsDataUnit output = sparkJobResult.getTargets().get(0);
            step.setCount(output.getCount());

            List<Schema> baseSchemas = getBaseSourceSchemas(step);
            step.setTargetSchema(getTargetSchema(output, sparkJobConfig, configuration, baseSchemas));

        } catch (Exception e) {
            log.error("Failed to transform data", e);
            return false;
        } finally {
            if (session != null) {
                livySessionService.stopSession(session);
            }
        }
        return true;
    }

    protected void modifySparkJobConfig(S sparkJobConfig, T stepConfig) {
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
        if (source instanceof TableSource) {
            TableSource tableSource = (TableSource) source;
            sourceTable = metadataProxy.getTable(tableSource.getCustomerSpace().toString(), tableSource.getTable()
                    .getName());
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
        log.info("Select source " + sourceName + "@versions " + StringUtils.join(versions, ","));
        return sourceTable.toHdfsDataUnit(sourceName);
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
            Schema resultSchema = AvroParquetUtils.parseAvroSchema(yarnConfiguration, result.getPath());
            return AvroUtils.overwriteFields(resultSchema, inputFields);
        } else {
            return null;
        }
    }

    private LivySession createLivySession(TransformStep step, TransformationProgress progress, //
                                          Map<String, String> sparkProps) {
        String creator = progress.getCreatedBy();
        String primaryJobName = creator + "~" + getName();
        String secondaryJobName = getSecondaryJobName(progress, step);
        String jobName = StringUtils.isNotBlank(secondaryJobName) //
                ? primaryJobName + "~" + secondaryJobName : primaryJobName;
        String livyHost;
        if (Boolean.TRUE.equals(useEmr)) {
            livyHost = emrCacheService.getLivyUrl();
        } else {
            livyHost = "http://localhost:8998";
        }
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(context -> livySessionService.startSession(livyHost, jobName, //
                getLivyConf(sparkProps), getSparkConf(sparkProps)));
    }

    @Override
    public String outputSubDir() {
        return "Output1";
    }

    private Map<String, Object> getLivyConf(Map<String, String> sparkProps) {
        Map<String, Object> conf = new HashMap<>();
        conf.put("driverCores", Integer.valueOf(sparkProps.getOrDefault("spark.driver.cores", driverCores)));
        conf.put("driverMemory", sparkProps.getOrDefault("spark.driver.memory", driverMem));
        conf.put("executorCores", Integer.valueOf(sparkProps.getOrDefault("spark.executor.cores", executorCores)));
        conf.put("executorMemory", sparkProps.getOrDefault("spark.executor.memory", executorMem));
        return conf;
    }

    private Map<String, String> getSparkConf(Map<String, String> sparkProps) {
        Map<String, String> conf = new HashMap<>();
        int numExecutors = sparkProps.containsKey("spark.executor.instances") ? //
                Math.max(1, Integer.valueOf(sparkProps.get("spark.executor.instances"))) : 1;
        int adjustedMinExecutors = Math.min(Integer.valueOf(minExecutors), numExecutors);
        if (sparkProps.containsKey("spark.dynamicAllocation.minExecutors")) {
            adjustedMinExecutors = Math.max(0, Integer.valueOf(sparkProps.get("spark.dynamicAllocation.minExecutors")));
        }
        int adjustedMaxExecutors = Math.max(Integer.valueOf(maxExecutors), numExecutors);
        if (sparkProps.containsKey("spark.dynamicAllocation.maxExecutors")) {
            int customMaxExecutors = Math.max(1, Integer.valueOf(sparkProps.get("spark.dynamicAllocation.maxExecutors")));
            if (customMaxExecutors >= adjustedMinExecutors) {
                adjustedMaxExecutors = customMaxExecutors;
            } else {
                log.warn("Customized max executors " + customMaxExecutors //
                        + " is too small, keep using " + adjustedMaxExecutors);
            }
        }
        numExecutors = Math.max(numExecutors, adjustedMinExecutors);
        conf.put("spark.executor.instances", String.valueOf(numExecutors));
        conf.put("spark.dynamicAllocation.minExecutors", String.valueOf(adjustedMinExecutors));
        conf.put("spark.dynamicAllocation.maxExecutors", String.valueOf(adjustedMaxExecutors));
        Set<String> skipKeys = Sets.newHashSet( //
                "spark.driver.cores", //
                "spark.driver.memory", //
                "spark.executor.cores", //
                "spark.executor.memory", //
                "spark.executor.instances", //
                "spark.dynamicAllocation.minExecutors", //
                "spark.dynamicAllocation.maxExecutors"
        );
        sparkProps.forEach((key, value) -> {
            if (!skipKeys.contains(key)) {
                conf.put(key, value);
            }
        });
        return conf;
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
