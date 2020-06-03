package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKET_TXMFR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CALC_STATS_TXMFR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_PROFILE_TXMFR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

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
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.domain.exposed.spark.stats.BucketEncodeConfig;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;
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
    protected TransformationProxy transformationProxy;

    @Inject
    private CloneTableService cloneTableService;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private BatonService batonService;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublishDynamo;

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
        return profile(masterTableName, false, false);
    }

    protected TransformationStepConfig profile(String masterTableName, List<ProfileParameters.Attribute> declaredAttrs) {
        TransformationStepConfig step = initStepWithInputTable(masterTableName, "CustomerUniverse");
        return configureProfileStep(step, declaredAttrs, false, false);
    }

    protected TransformationStepConfig profile(String masterTableName, boolean detectDiscrete, boolean detectCategorical) {
        TransformationStepConfig step = initStepWithInputTable(masterTableName, "CustomerUniverse");
        return configureProfileStep(step, null, detectDiscrete, detectCategorical);
    }

    protected TransformationStepConfig profile(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        return configureProfileStep(step, null, false, false);
    }

    private TransformationStepConfig configureProfileStep(TransformationStepConfig step,
                                                          List<ProfileParameters.Attribute> declaredAttrs,
                                                          boolean detectDiscrete, boolean detectCategorical) {
        step.setTransformer(TRANSFORMER_PROFILE_TXMFR);
        ProfileJobConfig conf = new ProfileJobConfig();
        conf.setDeclaredAttrs(declaredAttrs);
        conf.setAutoDetectDiscrete(detectDiscrete);
        conf.setAutoDetectCategorical(detectCategorical);
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

    private TransformationStepConfig configureBucketStep(TransformationStepConfig step, String outputTablePrefix) {
        step.setTransformer(TRANSFORMER_BUCKET_TXMFR);

        if (StringUtils.isNotBlank(outputTablePrefix)) {
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(outputTablePrefix);
            targetTable.setExpandBucketedAttrs(true);
            step.setTargetTable(targetTable);
        }
        BucketEncodeConfig config = new BucketEncodeConfig();
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig calcStats(int profileStep, String inputTableName, String statsTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(profileStep));
        addBaseTables(step, inputTableName);
        step.setTransformer(TRANSFORMER_CALC_STATS_TXMFR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalcStatsConfig conf = new CalcStatsConfig();
        step.setConfiguration(appendEngineConf(conf, lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig calcStats(int profileStep, int inputStep, String statsTablePrefix) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(inputStep, profileStep));
        step.setTransformer(TRANSFORMER_CALC_STATS_TXMFR);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(statsTablePrefix);
        step.setTargetTable(targetTable);

        CalcStatsConfig conf = new CalcStatsConfig();
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
        String distKey = tableRole.getDistKey();
        List<String> sortKeys = new ArrayList<>(tableRole.getSortKeys());

        String partition = null;
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus != null && dcStatus.getDetail() != null) {
            partition = dcStatus.getRedshiftPartition();
        }

        RedshiftExportConfig config = new RedshiftExportConfig();
        config.setTableName(tableName);
        config.setDistKey(distKey);
        config.setSortKeys(sortKeys);
        config.setInputPath(getInputPath(tableName) + "/*.avro");
        config.setClusterPartition(partition);
        config.setUpdateMode(false);

        Table summary = metadataProxy.getTableSummary(customerSpace.toString(), tableName);
        if (CollectionUtils.isNotEmpty(summary.getExtracts())) {
            Extract extract = summary.getExtracts().get(0);
            Long count = extract.getProcessedRecords();
            if (count != null && count > 0) {
                config.setExpectedCount(count);
            }
        }

        addToListInContext(TABLES_GOING_TO_REDSHIFT, config, RedshiftExportConfig.class);
    }

    /**
     * Try to get batch store for {@link this#getEntity()} in inactive version. If
     * table not exist in inactive version, link the table from active version.
     *
     * @return batch store table name
     */
    protected String ensureInactiveBatchStoreExists() {
        return ensureInactiveTableRoleExists(getEntity().getBatchStore(), "batch store", getEntity().name());
    }

    protected String ensureInactiveTableRoleExists(TableRoleInCollection role, String targetDisplayName,
            String entity) {
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        DataCollection.Version inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactive);
        if (StringUtils.isBlank(tableName)) {
            tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
            if (StringUtils.isNotBlank(tableName)) {
                log.info("Found the {} for entity {} in active version {}: {}", targetDisplayName, entity, active,
                        tableName);
                cloneTableService.setActiveVersion(active);
                cloneTableService.setCustomerSpace(customerSpace);
                DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
                if (dcStatus != null && dcStatus.getDetail() != null) {
                    cloneTableService.setRedshiftPartition(dcStatus.getRedshiftPartition());
                }
                cloneTableService.linkInactiveTable(role);
            }
        } else {
            log.info("Found the {} for entity {} in inactive version {}: {}", targetDisplayName, entity, inactive,
                    tableName);
        }
        return tableName;
    }

    protected void exportToDynamo(String tableName, String partitionKey, String sortKey) {
        if (!skipPublishDynamo) {
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
                        + "\" from Period Proxy as an ISO formatted date", e);
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

    protected boolean shouldExcludeDataCloudAttrs() {
        String tenantId = configuration.getCustomerSpace().getTenantId();
        return batonService.shouldExcludeDataCloudAttrs(tenantId);
    }

}
