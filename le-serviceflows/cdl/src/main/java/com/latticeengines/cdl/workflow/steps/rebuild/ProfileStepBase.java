package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class ProfileStepBase<T extends BaseWrapperStepConfiguration> extends BaseTransformWrapperStep<T> {

    private static final Logger log = LoggerFactory.getLogger(ProfileStepBase.class);

    // The date that the Process/Analyze pipeline was run as a string.
    protected String evaluationDateStr = null;
    // The timestamp representing the beginning of the day that the Process/Analyze pipeline was run.  Used for date
    // attribute profiling.
    protected Long evaluationDateAsTimestamp = null;
    // The date format pattern desired by the UI for Last Data Refresh Attribute field.
    private static final DateTimeFormatter REFRESH_DATE_FORMATTER = DateTimeFormatter.ofPattern("MMMM d, yyyy");

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected PeriodProxy periodProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    protected TransformationProxy transformationProxy;

    protected abstract BusinessEntity getEntity();

    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(getEntity(), key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    protected TransformationStepConfig profile(String masterTableName) {
        TransformationStepConfig step = initStepWithInputTable(masterTableName, "CustomerUniverse");
        return configureProfileStep(step);
    }

    protected TransformationStepConfig profile(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        return configureProfileStep(step);
    }

    private TransformationStepConfig configureProfileStep(TransformationStepConfig step) {
        step.setTransformer(TRANSFORMER_PROFILER);
        ProfileConfig conf = new ProfileConfig();
        conf.setEncAttrPrefix(CEAttr);
        if (evaluationDateAsTimestamp != null) {
            conf.setEvaluationDateAsTimestamp(evaluationDateAsTimestamp);
        }
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    protected TransformationStepConfig bucket(int profileStep, String masterTableName, String outputTablePrefix) {
        TransformationStepConfig step = initStepWithInputTable(masterTableName, "CustomerUniverse");
        step.setInputSteps(Collections.singletonList(profileStep));
        return configureBucketStep(step, outputTablePrefix);
    }

    protected TransformationStepConfig bucket(int profileStep, int inputStep, String outputTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(profileStep, inputStep));
        return configureBucketStep(step, outputTablePrefix);
    }

    private TransformationStepConfig configureBucketStep(TransformationStepConfig step, String outputTablePrefix) {
        step.setTransformer(TRANSFORMER_BUCKETER);

        if (StringUtils.isNotBlank(outputTablePrefix)) {
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(outputTablePrefix);
            targetTable.setExpandBucketedAttrs(true);
            step.setTargetTable(targetTable);
        }

        step.setConfiguration(emptyStepConfig(lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig calcStats(int profileStep, int bucketStep, String statsTablePrefix,
            List<String> dedupFields) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(bucketStep, profileStep));
        step.setTransformer(TRANSFORMER_STATS_CALCULATOR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalculateStatsConfig conf = new CalculateStatsConfig();
        if (CollectionUtils.isNotEmpty(dedupFields)) {
            conf.setDedupFields(dedupFields);
        }
        step.setConfiguration(appendEngineConf(conf, lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig sort(String inputTableName, String outputTablePrefix, String sortKey,
            int partitions) {
        TransformationStepConfig step = initStepWithInputTable(inputTableName, "Contacts");
        return configSortStep(step, outputTablePrefix, sortKey, partitions);
    }

    protected TransformationStepConfig sort(int inputStep, String outputTablePrefix, String sortKey, int partitions) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<Integer> inputSteps = Collections.singletonList(inputStep);
        step.setInputSteps(inputSteps);
        return configSortStep(step, outputTablePrefix, sortKey, partitions);
    }

    private TransformationStepConfig configSortStep(TransformationStepConfig step, String outputTablePrefix,
            String sortKey, int partitions) {
        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(outputTablePrefix);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig conf = new SorterConfig();
        conf.setPartitions(partitions);
        if (partitions > 1) {
            conf.setSplittingThreads(maxSplitThreads);
            conf.setSplittingChunkSize(10000L);
        }
        conf.setCompressResult(true);
        // TODO: only support single sort key now
        conf.setSortingField(sortKey);
        String confStr = appendEngineConf(conf, lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    protected TransformationStepConfig initStepWithInputTable(String inputTableName, String tableSourceName) {
        TransformationStepConfig step = new TransformationStepConfig();
        SourceTable sourceTable = new SourceTable(inputTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        return step;
    }

    protected String renameServingStoreTable(Table servingStoreTable) {
        return renameServingStoreTable(getEntity(), servingStoreTable);
    }

    protected String renameServingStoreTable(BusinessEntity servingEntity, Table servingStoreTable) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String prefix = String.join("_", customerSpace.getTenantId(), servingEntity.name());
        String goodName = NamingUtils.timestamp(prefix);
        log.info("Renaming table " + servingStoreTable.getName() + " to " + goodName);
        metadataProxy.renameTable(customerSpace.toString(), servingStoreTable.getName(), goodName);
        servingStoreTable.setName(goodName);
        return goodName;
    }

    protected void exportTableRoleToRedshift(String tableName, TableRoleInCollection tableRole) {
        String distKey = tableRole.getPrimaryKey().name();
        List<String> sortKeys = new ArrayList<>(tableRole.getForeignKeysAsStringList());
        if (!sortKeys.contains(tableRole.getPrimaryKey().name())) {
            sortKeys.add(tableRole.getPrimaryKey().name());
        }
        RedshiftExportConfig config = new RedshiftExportConfig();
        config.setTableName(tableName);
        config.setDistKey(distKey);
        config.setSortKeys(sortKeys);
        config.setInputPath(getInputPath(tableName) + "/*.avro");
        config.setUpdateMode(false);

        addToListInContext(TABLES_GOING_TO_REDSHIFT, config, RedshiftExportConfig.class);
    }

    protected void exportToDynamo(String tableName, String partitionKey, String sortKey) {
        String inputPath = getInputPath(tableName);
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tableName);
        config.setInputPath(inputPath);
        config.setPartitionKey(partitionKey);
        if (StringUtils.isNotBlank(sortKey)) {
            config.setSortKey(sortKey);
        }

        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

    private String getInputPath(String tableName) {
        return metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(), tableName);
    }

    protected void setEvaluationDateStrAndTimestamp() {
        // Convert the evaluation date (generally the current date which is when the pipeline is running) to a
        // timestamp.
        evaluationDateStr = findEvaluationDate();
        LocalDate evaluationDate;
        if (!StringUtils.isBlank(evaluationDateStr)) {
            try {
                evaluationDate = LocalDate.parse(evaluationDateStr, DateTimeFormatter.ISO_DATE);
            } catch (DateTimeParseException e) {
                log.error("Could not parse evaluation date string \"" + evaluationDateStr
                        + "\" from Period Proxy as an ISO formatted date");
                log.error("Error is: " + e.getLocalizedMessage());
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                log.error(sw.toString());
                evaluationDate = LocalDate.now();
                evaluationDateStr = evaluationDate.format(REFRESH_DATE_FORMATTER);
            }
        } else {
            log.warn("Evaluation Date from Period Proxy is blank.  Profile Account will generate date");
            evaluationDate = LocalDate.now();
            evaluationDateStr = evaluationDate.format(REFRESH_DATE_FORMATTER);
        }
        evaluationDateAsTimestamp = evaluationDate.atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
        log.info("Evaluation date for Profile Account date attributes: " + evaluationDateStr);
        log.info("Evaluation timestamp for Profile Account date attributes: " + evaluationDateAsTimestamp);
    }

    protected String findEvaluationDate() {
        String evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
        if (StringUtils.isBlank(evaluationDate)) {
            log.error("Failed to find evaluation date from workflow context");
            evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
            if (StringUtils.isBlank(evaluationDate)) {
                log.error("Failed to get evaluation date from Period Proxy.");
            }
        }
        return evaluationDate;
    }

}
