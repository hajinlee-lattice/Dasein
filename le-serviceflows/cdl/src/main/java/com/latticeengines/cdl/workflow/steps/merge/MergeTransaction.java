package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOFT_DELETE_TXFMR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema.Type;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.DataLimit;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodDataCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodDateConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransactionStandardizerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.serviceflows.workflow.util.TableCloneUtils;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.yarn.exposed.service.EMREnvService;


@Component(MergeTransaction.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeTransaction extends BaseMergeImports<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeTransaction.class);

    static final String BEAN_NAME = "mergeTransaction";

    private Table rawTable;
    private int dailyStep, mergeRawStep, standardizeStep, dayPeriodStep, softDeleteMergeStep, softDeleteStep;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private LivySessionService sessionService;

    @Inject
    private SparkJobService sparkJobService;

    private List<String> stringFields;
    private List<String> longFields;
    private List<String> intFields;

    private boolean schemaChanged;

    private String mergedImportTable;

    private boolean entityMatchEnabled;

    private boolean emptyRawStore;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();

        entityMatchEnabled = configuration.isEntityMatchEnabled();
        if (entityMatchEnabled) {
            log.info("Entity match is enabled for transaction merge");
        }

        mergedBatchStoreName = TableRoleInCollection.ConsolidatedRawTransaction.name() + "_Merged";
        if (softDeleteEntities.containsKey(entity) && Boolean.TRUE.equals(softDeleteEntities.get(entity))) {
            rawTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                    TableRoleInCollection.ConsolidatedRawTransaction, inactive);
            if (rawTable == null) {
                buildPeriodStore(TableRoleInCollection.ConsolidatedRawTransaction, SchemaInterpretation.TransactionRaw);
                emptyRawStore = true;
            }
        } else {
            initOrCloneRawStore(TableRoleInCollection.ConsolidatedRawTransaction, SchemaInterpretation.TransactionRaw);
        }
        rawTable = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        if (rawTable == null) {
            throw new IllegalStateException("Cannot find raw period store");
        }
        log.info("Found rawTable " + rawTable.getName() + " @version " + inactive);

        stringFields = new ArrayList<>();
        longFields = new ArrayList<>();
        intFields = new ArrayList<>();
        Table rawTemplate = SchemaRepository.instance().getSchema(SchemaInterpretation.TransactionRaw, true,
                entityMatchEnabled);
        getTableFields(rawTemplate, stringFields, longFields, intFields);

        List<String> curStringFields = new ArrayList<>();
        List<String> curLongFields = new ArrayList<>();
        List<String> curIntFields = new ArrayList<>();
        getTableFields(rawTable, curStringFields, curLongFields, curIntFields);

        schemaChanged = compareFields(stringFields, curStringFields);
        if (!schemaChanged) {
            schemaChanged = compareFields(longFields, curLongFields);
        }
        if (!schemaChanged) {
            schemaChanged = compareFields(intFields, curIntFields);
        }

        if (schemaChanged) {
            log.info("Detected schema change. Updating raw table");
            rawTable.setAttributes(rawTemplate.getAttributes());
            metadataProxy.updateTable(customerSpace.toString(), rawTable.getName(), rawTable);
            dataCollectionProxy.upsertTable(customerSpace.toString(), rawTable.getName(),
                    TableRoleInCollection.ConsolidatedRawTransaction, inactive);
        }

        mergedImportTable = getStringValueFromContext(ENTITY_MATCH_TXN_TARGETTABLE);
        if (StringUtils.isBlank(mergedImportTable)) {// && skipSoftDelete) {
            throw new RuntimeException("There's no matched table found!");
        } else {
            double oldTableSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, rawTable);
            Table tableSummary = metadataProxy.getTableSummary(customerSpace.toString(), mergedImportTable);
            double newTableSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, tableSummary);
            scalingMultiplier = ScalingUtils.getMultiplier(oldTableSize + newTableSize);
            log.info(String.format("Adjust scalingMultiplier=%d based on the size of two tables %.2f g.", //
                    scalingMultiplier, oldTableSize + newTableSize));
        }
    }

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MergeTransaction");
        List<TransformationStepConfig> steps;
        if (schemaChanged) {
            steps = getUpgradeSteps();
        } else {
            steps = getSteps();
        }
        request.setSteps(steps);
        return request;
    }

    @Override
    protected void onPostTransformationCompleted() {
        String diffTableName = TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
        Table diffTable = metadataProxy.getTable(
                customerSpace.toString(), diffTableName);
        isDataQuotaLimit();
        addToListInContext(TEMPORARY_CDL_TABLES, diffTableName, String.class);
        updateEntityValueMapInContext(ENTITY_DIFF_TABLES, diffTableName, String.class);
        generateDiffReport();
        updateEarliestLatestTransaction();
        enrichTableSchema(diffTable, null);
        enrichTableSchema(rawTable, TableRoleInCollection.ConsolidatedRawTransaction);
    }

    private void getTableFields(Table table, List<String> stringFields, List<String> longFields, List<String> intFields) {
        List<Attribute> attrs = table.getAttributes();
        for (Attribute attr: attrs) {
            Type attrType = Type.valueOf(attr.getPhysicalDataType());
            String attrName = attr.getName();
            if (attrType == Type.STRING) {
                stringFields.add(attrName);
            } else if (attrType == Type.LONG) {
                longFields.add(attrName);
            } else if (attrType == Type.INT) {
                intFields.add(attrName);
            } else {
                log.warn("Invalid attribute type for " + attrName);
            }
        }
        Collections.sort(stringFields);
        Collections.sort(longFields);
        Collections.sort(intFields);
    }

    private boolean compareFields(List<String> fields, List<String> curFields) {
        if (fields.size() != curFields.size()) {
            return true;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (!(fields.get(i).equals(curFields.get(i)))) {
                return true;
            }
        }
        return false;
    }


    private List<TransformationStepConfig> getUpgradeSteps() {
        dailyStep = 0;
        mergeRawStep = 1;
        standardizeStep = 2;
        dayPeriodStep = 3;
        softDeleteMergeStep = dayPeriodStep + 3;
        softDeleteStep = softDeleteMergeStep + 1;
        int collectRawStep = softDeleteStep + 1;
        int cleanupAgainStep = collectRawStep + 1;
        int dayPeriodAgainStep = cleanupAgainStep + 1;


        TransformationStepConfig daily = addTrxDate();
        TransformationStepConfig rawMerge = mergeRaw();
        TransformationStepConfig standardize = standardizeTrx(mergeRawStep);
        TransformationStepConfig dayPeriods = collectDays(standardizeStep);
        TransformationStepConfig rawCleanup = cleanupRaw(rawTable, dayPeriodStep);
        TransformationStepConfig dailyPartition = partitionDaily(dayPeriodStep, standardizeStep);

        TransformationStepConfig report = reportDiff(mergedImportTable);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(daily);
        steps.add(rawMerge);
        steps.add(standardize);
        steps.add(dayPeriods);
        steps.add(rawCleanup);
        steps.add(dailyPartition);
        steps.add(report);
        return steps;
    }

    private TransformationStepConfig softDelete(int softDeleteMergeStep, Table rawTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_SOFT_DELETE_TXFMR);
        step.setInputSteps(Collections.singletonList(softDeleteMergeStep));

        String tableSourceName = "RawTransaction";
        String sourceTableName = rawTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        SoftDeleteConfig softDeleteConfig = new SoftDeleteConfig();
        softDeleteConfig.setDeleteSourceIdx(0);
        softDeleteConfig.setIdColumn(InterfaceName.AccountId.name());
        step.setConfiguration(appendEngineConf(softDeleteConfig, lightEngineConfig()));
        return step;

    }

    private List<TransformationStepConfig> getSteps() {
        dailyStep = 0;
        standardizeStep = 1;
        dayPeriodStep = 2;

        TransformationStepConfig daily = addTrxDate();
        TransformationStepConfig standardize = standardizeTrx(dailyStep);
        TransformationStepConfig dayPeriods = collectDays(standardizeStep);
        TransformationStepConfig dailyPartition = partitionDaily(dayPeriodStep, standardizeStep);
        TransformationStepConfig report = reportDiff(mergedImportTable);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(daily);
        steps.add(standardize);
        steps.add(dayPeriods);
        steps.add(dailyPartition);
        steps.add(report);
        return steps;
    }


    private TransformationStepConfig addTrxDate() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATE_CONVERTOR);

        List<String> baseSources = Collections.singletonList(mergedImportTable);
        Map<String, SourceTable> baseTables;
        baseTables = new HashMap<>();
        SourceTable sourceMasterTable = new SourceTable(mergedImportTable, customerSpace);
        baseTables.put(mergedImportTable, sourceMasterTable);
        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);

        PeriodDateConvertorConfig config = new PeriodDateConvertorConfig();
        config.setTrxTimeField(InterfaceName.TransactionTime.name());
        config.setTrxDateField(InterfaceName.TransactionDate.name());
        config.setTrxDayPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig mergeRaw() {
        TransformationStepConfig step = new TransformationStepConfig();
        addBaseTables(step, rawTable.getName());
        step.setInputSteps(Collections.singletonList(dailyStep));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DATA);
        step.setConfiguration(appendEngineConf(getConsolidateDataTxmfrConfig(false, false, true), lightEngineConfig()));

        return step;
    }

    private TransformationStepConfig standardizeTrx(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.TRANSACTION_STANDARDIZER);
        step.setInputSteps(Collections.singletonList(inputStep));
        TransactionStandardizerConfig config = new TransactionStandardizerConfig();
        config.setStringFields(stringFields);
        config.setLongFields(longFields);
        config.setIntFields(intFields);
        config.setCustomField(InterfaceName.CustomTrxField.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig collectDays(int standardizeStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);
        step.setInputSteps(Collections.singletonList(standardizeStep));
        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(diffTablePrefix);
        step.setTargetTable(targetTable);

        step.setConfiguration(appendEngineConf(config, heavyMemoryEngineConfig()));
        return step;
    }

    private TransformationStepConfig collectRaw() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_COLLECTOR);

        List<String> sourceNames = new ArrayList<>();
        Map<String, SourceTable> sourceTables = new HashMap<>();
        sourceNames.add(rawTable.getName());
        SourceTable sourceTable = new SourceTable(rawTable.getName(), customerSpace);
        sourceTables.put(rawTable.getName(), sourceTable);

        PeriodCollectorConfig config = new PeriodCollectorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());

        step.setBaseSources(sourceNames);
        step.setBaseTables(sourceTables);
        step.setConfiguration(appendEngineConf(config, heavyMemoryEngineConfig()));
        return step;
    }

    private TransformationStepConfig cleanupRaw(Table periodTable, int dayPeriodStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_CLEANER);
        step.setInputSteps(Collections.singletonList(dayPeriodStep));

        String tableSourceName = "PeriodTable";
        String sourceTableName = periodTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        PeriodDataCleanerConfig config = new PeriodDataCleanerConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private TransformationStepConfig partitionDaily(int dayPeriodStep, int standardizeStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.PERIOD_DATA_DISTRIBUTOR);
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(dayPeriodStep);
        inputSteps.add(standardizeStep);
        step.setInputSteps(inputSteps);

        String tableSourceName = "RawTransaction";
        String sourceTableName = rawTable.getName();
        SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);

        PeriodDataDistributorConfig config = new PeriodDataDistributorConfig();
        config.setPeriodField(InterfaceName.TransactionDayPeriod.name());
        // If raw store doesn't exist before: if some periods fail during
        // distribution, they could be deleted and retry
        // If raw store exists: period data is appended to existing period file,
        // if some periods fail during distribution, cannot retry due to
        // existing period data could be lost
        config.setRetryable(emptyRawStore);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    private void initOrCloneRawStore(TableRoleInCollection role, SchemaInterpretation schema) {
        String activeTableName = "";
        boolean isEntityMatchRematch =
                configuration.isEntityMatchEnabled() && Boolean.TRUE.equals(getObjectFromContext(FULL_REMATCH_PA, Boolean.class));
        if (!Boolean.TRUE.equals(configuration.getNeedReplace()) && !isEntityMatchRematch) {
            activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, active);
        }
        // in replace mode, delete the records in document db
        if (Boolean.TRUE.equals(configuration.getNeedReplace())) {
            cdlAttrConfigProxy.removeAttrConfigByTenantAndEntity(customerSpace.toString(), configuration.getMainEntity());
        }
        if (StringUtils.isNotBlank(activeTableName)) {
            log.info("Cloning {} from {} to {}.", role, active, inactive);
            clonePeriodStore(role);
        } else {
            log.info("Building a brand new {}.", role);
            buildPeriodStore(role, schema);
            emptyRawStore = true;
        }
    }

    private void clonePeriodStore(TableRoleInCollection role) {
        Table activeTable = dataCollectionProxy.getTable(customerSpace.toString(), role, active);
        String cloneName = NamingUtils.timestamp(role.name());
        String queue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        queue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
        Table inactiveTable = TableCloneUtils //
                .cloneDataTable(yarnConfiguration, customerSpace, cloneName, activeTable, queue);
        metadataProxy.createTable(customerSpace.toString(), cloneName, inactiveTable);
        dataCollectionProxy.upsertTable(customerSpace.toString(), cloneName, role, inactive);
    }

    private Table buildPeriodStore(TableRoleInCollection role, SchemaInterpretation schema) {
        Table table = SchemaRepository.instance().getSchema(schema, true, entityMatchEnabled);
        String tableName = NamingUtils.timestamp(role.name());
        table.setName(tableName);
        String hdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "").toString();

        try {
            log.info("Initialize period store " + hdfsPath + "/" + tableName);
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath + "/" + tableName);
        } catch (Exception e) {
            log.error("Failed to initialize period store " + hdfsPath + "/" + tableName);
            throw new RuntimeException("Failed to create period store " + role);
        }

        Extract extract = new Extract();
        extract.setName("extract_target");
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        extract.setProcessedRecords(1L);
        extract.setPath(hdfsPath + "/" + tableName + "/");
        table.setExtracts(Collections.singletonList(extract));
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), role, inactive);
        log.info("Upsert table " + table.getName() + " to role " + role + "version " + inactive);

        return table;
    }

    private void updateEarliestLatestTransaction() {
        DataFeed feed = dataFeedProxy.getDataFeed(customerSpace.toString());

        Pair<Integer, Integer> minMaxPeriod = TimeSeriesUtils.getMinMaxPeriod(yarnConfiguration, rawTable);
        Integer earliestDayPeriod = minMaxPeriod.getLeft();
        Integer latestDayPeriod = minMaxPeriod.getRight();
        Integer currentEarliest = feed.getEarliestTransaction();
        Integer currentLatest = feed.getLatestTransaction();
        Integer newEarliest = currentEarliest == null || earliestDayPeriod < currentEarliest ? earliestDayPeriod
                : currentEarliest;
        Integer newLatest = currentLatest == null || latestDayPeriod > currentLatest ? latestDayPeriod : currentLatest;
        if (newEarliest != currentEarliest || newLatest != currentLatest) {
            dataFeedProxy.updateEarliestLatestTransaction(customerSpace.toString(), newEarliest, newLatest);
        }

        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        detail.setMaxTxnDate(earliestDayPeriod);
        detail.setMinTxnDate(latestDayPeriod);
        log.info("MergeTransaction step : dataCollection Status is " + JsonUtils.serialize(detail));
        putObjectInContext(CDL_COLLECTION_STATUS, detail);
    }

    private void enrichTableSchema(Table table, TableRoleInCollection tableRole) {
        if (configuration.isEntityMatchEnabled()) {
            table.getAttributes().forEach(attr -> {
                // update metadata for AccountId attribute since it is only
                // created after transaction match and does not have the correct
                // metadata
                if (InterfaceName.AccountId.name().equals(attr.getName())) {
                    attr.setInterfaceName(InterfaceName.AccountId);
                    attr.setDisplayName("Atlas Account ID");
                    // TODO: Not marked as internal for existing daily/period
                    // txn store. Not sure for entity match. Need to check with
                    // PM
                    // attr.setTags(Tag.INTERNAL);
                    attr.setLogicalDataType(LogicalDataType.Id);
                    attr.setNullable(false);
                    attr.setApprovedUsage(ApprovedUsage.NONE);
                    attr.setSourceLogicalDataType(attr.getPhysicalDataType());
                    attr.setCategory(Category.DEFAULT.name());
                    attr.setSubcategory("Other");
                    attr.setFundamentalType(FundamentalType.ALPHA.getName());
                }
            });
        }
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
        if (tableRole != null) {
            dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(), tableRole, inactive);
        }
    }

    private void isDataQuotaLimit() {
        if (rawTable == null) {
            return;
        }
        List<Extract> extracts = rawTable.getExtracts();
        if (!CollectionUtils.isEmpty(extracts)) {
            Long dataCount = countRawTransactionInHdfs();
            DataLimit dataLimit = getObjectFromContext(DATAQUOTA_LIMIT, DataLimit.class);
            Long transactionDataQuotaLimit = dataLimit.getTransactionDataQuotaLimit();
            if (transactionDataQuotaLimit < dataCount) {
                throw new IllegalStateException("the " + configuration.getMainEntity() + " data quota limit is " + transactionDataQuotaLimit +
                        ", The data you uploaded has exceeded the limit.");
            }
            log.info("stored data is {}, the {} data limit is {}.",
                    dataCount, configuration.getMainEntity(),
                    transactionDataQuotaLimit);
        }
    }

    private long countRawTransactionInHdfs() {
        try {
            Long result = 0L;
            List<Extract> extracts = rawTable.getExtracts();
            if (CollectionUtils.isEmpty(extracts)) {
                return 0L;
            }
            for (Extract extract : extracts) {
                String hdfsPath = extract.getPath();
                if (!hdfsPath.endsWith("*.avro")) {
                    if (hdfsPath.endsWith("/")) {
                        hdfsPath += "*.avro";
                    } else {
                        hdfsPath += "/*.avro";
                    }
                }
                log.info("Count records in HDFS {}.", hdfsPath);
                result += SparkUtils.countRecordsInGlobs(sessionService, sparkJobService, yarnConfiguration, hdfsPath);
                log.info("Table role {} has {} entities, hdfs path is {}.",
                        TableRoleInCollection.ConsolidatedRawTransaction,
                        result, extract.getPath());
            }
            return result;
        } catch (Exception ex) {
            log.error("Fail to count raw entities in table.", ex);
            return 0L;
        }
    }


}
