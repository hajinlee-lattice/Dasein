package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateDataTransformerConfig.ConsolidateDataTxmfrConfigBuilder;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;
import com.latticeengines.serviceflows.workflow.util.ETLEngineLoad;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

public abstract class BaseMergeImports<T extends BaseProcessEntityStepConfiguration>
        extends BaseTransformWrapperStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseMergeImports.class);

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private MatchProxy matchProxy;

    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected BusinessEntity entity;
    protected TableRoleInCollection batchStore;
    protected String batchStoreTablePrefix;
    protected String mergedBatchStoreName;
    protected String diffTablePrefix;
    protected String diffReportTablePrefix;
    protected String batchStorePrimaryKey;
    protected List<String> batchStoreSortKeys;

    protected List<Table> inputTables = new ArrayList<>();
    protected List<String> inputTableNames = new ArrayList<>();
    protected Table masterTable;
    boolean skipSoftDelete = true;
    List<Action> softDeleteActions;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        generateDiffReport();
    }

    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        softDeleteActions = getListObjectFromContext(SOFT_DEELETE_ACTIONS, Action.class);
        skipSoftDelete = CollectionUtils.isEmpty(softDeleteActions);

        entity = configuration.getMainEntity();
        batchStore = entity.getBatchStore();
        if (batchStore != null) {
            batchStoreTablePrefix = entity.name();
            batchStorePrimaryKey = batchStore.getPrimaryKey().name();
            batchStoreSortKeys = batchStore.getForeignKeysAsStringList();
            mergedBatchStoreName = batchStore.name() + "_Merged";
        }
        diffTablePrefix = entity.name() + "_Diff";
        diffReportTablePrefix = entity.name() + "_DiffReport";

        @SuppressWarnings("rawtypes")
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        if (entityImportsMap == null) {
            return;
        }

        List<DataFeedImport> imports = JsonUtils.convertList(entityImportsMap.get(entity), DataFeedImport.class);
        if (CollectionUtils.isNotEmpty(imports)) {
            List<Table> tables = imports.stream().map(DataFeedImport::getDataTable).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(tables)) {
                throw new RuntimeException("There is no input tables to consolidate.");
            }
            Collections.reverse(tables);
            tables.sort(Comparator.comparing((Table t) -> t.getLastModifiedKey() == null ? -1
                    : t.getLastModifiedKey().getLastModifiedTimestamp() == null ? -1
                            : t.getLastModifiedKey().getLastModifiedTimestamp())
                    .reversed());
            for (Table table : tables) {
                inputTables.add(table);
                inputTableNames.add(table.getName());
            }
            setScalingMultiplier(tables);
        }
    }

    protected TransformationStepConfig reportDiff(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_REPORT);
        step.setInputSteps(Collections.singletonList(inputStep));
        configReportDiffStep(step);
        return step;
    }

    protected TransformationStepConfig reportDiff(String diffTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_REPORT);
        addBaseTables(step, diffTableName);
        configReportDiffStep(step);
        return step;
    }

    private void configReportDiffStep(TransformationStepConfig step) {
        ConsolidateReportConfig config = new ConsolidateReportConfig();
        config.setEntity(entity);
        config.setThresholdTime(getLongValueFromContext(NEW_RECORD_CUT_OFF_TIME));
        String configStr = appendEngineConf(config, lightEngineConfig());
        step.setConfiguration(configStr);
        setTargetTable(step, diffReportTablePrefix);
    }

    TransformationStepConfig dedupAndConcatImports(String joinKey) {
        return dedupAndConcatTables(joinKey, true, inputTableNames);
    }

    TransformationStepConfig dedupAndConcatTables(String joinKey, boolean dedupSrc, List<String> tables) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        tables.forEach(tblName -> addBaseTables(step, tblName));

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(dedupSrc);
        config.setJoinKey(joinKey);
        config.setAddTimestamps(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    TransformationStepConfig concatImports(String targetTablePrefix, String[][] cloneSrcFlds,
            String[][] renameSrcFlds, String convertBatchStoreTableName, int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        if (inputStep == -1) {
            inputTableNames.forEach(tblName -> addBaseTables(step, tblName));
        } else {
            step.setInputSteps(Collections.singletonList(inputStep));
        }

        if (StringUtils.isNotEmpty(convertBatchStoreTableName)) {
            addBaseTables(step, convertBatchStoreTableName);
        }

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(false);
        config.setJoinKey(null);
        config.setAddTimestamps(false);
        config.setCloneSrcFields(cloneSrcFlds);
        config.setRenameSrcFields(renameSrcFlds);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        if (StringUtils.isNotBlank(targetTablePrefix)) {
            setTargetTable(step, targetTablePrefix);
        }

        return step;
    }

    TransformationStepConfig mergeSoftDelete(List<Action> softDeleteActions) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        softDeleteActions.forEach(action -> {
            DeleteActionConfiguration configuration = (DeleteActionConfiguration) action.getActionConfiguration();
            addBaseTables(step, configuration.getDeleteDataTable());
        });
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(InterfaceName.AccountId.name());
        config.setAddTimestamps(false);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    TransformationStepConfig dedupAndMerge(String joinKey, List<Integer> inputSteps, List<String> inputTables,
                                           List<String> requiredIdCols) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        if (CollectionUtils.isEmpty(inputSteps) && CollectionUtils.isEmpty(inputTables)) {
            throw new IllegalArgumentException("No input to be merged.");
        }
        if (CollectionUtils.isNotEmpty(inputSteps)) {
            step.setInputSteps(inputSteps);
        }
        if (CollectionUtils.isNotEmpty(inputTables)) {
            inputTables.forEach(tblName -> addBaseTables(step, tblName));
        }
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(joinKey);
        config.setAddTimestamps(true);

        // those Id columns must exist in the output
        if (CollectionUtils.isNotEmpty(requiredIdCols)) {
            Map<String, String> requiredCols = new HashMap<>();
            requiredIdCols.forEach(col -> requiredCols.put(col, "string"));
            config.setRequiredColumns(requiredCols);
        }

        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig mergeInputs(ConsolidateDataTransformerConfig config,
            String targetTableNamePrefix, ETLEngineLoad engineLoad, String convertBatchStoreTableName, int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        if (inputStep == -1) {
            inputTableNames.forEach(tblName -> addBaseTables(step, tblName));
        } else {
            step.setInputSteps(Collections.singletonList(inputStep));
        }
        if (StringUtils.isNotEmpty(convertBatchStoreTableName)) {
            addBaseTables(step, convertBatchStoreTableName);
        }

        if (targetTableNamePrefix != null) {
            setTargetTable(step, targetTableNamePrefix);
        }
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONSOLIDATE_DATA);
        step.setConfiguration(appendEngineConf(config, getEngineConfig(engineLoad)));
        return step;
    }

    TransformationStepConfig match(int inputStep, String matchTargetTable, String matchConfig) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        if (matchTargetTable != null) {
            setTargetTable(step, matchTargetTable);
        }
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(matchConfig);
        return step;
    }

    MatchInput getBaseMatchInput() {
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(false);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        return matchInput;
    }

    /**
     * Retrieve all system IDs for target entity of current tenant (sorted by system
     * priority from high to low)
     *
     * @param entity
     *            target entity
     * @return non-null list of system IDs
     */
    protected List<String> getSystemIds(BusinessEntity entity) {
        Map<String, List<String>> systemIdMap = configuration.getSystemIdMap();
        if (MapUtils.isEmpty(systemIdMap)) {
            return Collections.emptyList();
        }
        return systemIdMap.getOrDefault(entity.name(), Collections.emptyList());
    }

    // For common use case. If need to customize more parameters, better to
    // build ConsolidateDataTransformerConfig in workflow step itself instead of
    // making shared method with large signature
    protected ConsolidateDataTransformerConfig getConsolidateDataTxmfrConfig(boolean dedupeSource,
            boolean addTimettamps, boolean mergeOnly) {
        ConsolidateDataTxmfrConfigBuilder builder = new ConsolidateDataTxmfrConfigBuilder();
        builder.dedupeSource(dedupeSource);
        builder.addTimestamps(addTimettamps);
        builder.mergeOnly(mergeOnly);
        builder.srcIdField(InterfaceName.Id.name());
        builder.masterIdField(batchStorePrimaryKey);
        return builder.build();
    }

    protected void generateDiffReport() {
        String diffReportTableName = TableUtils.getFullTableName(diffReportTablePrefix, pipelineVersion);
        Table diffReport = metadataProxy.getTable(customerSpace.toString(), diffReportTableName);
        if (diffReport != null) {
            ObjectNode report = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                    ObjectNode.class);
            updateReportPayload(report, diffReport);
            putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), report);
            metadataProxy.deleteTable(customerSpace.toString(), diffReport.getName());
        } else {
            log.warn("Didn't find ConsolidationSummaryReport table for entity " + entity);
        }
        markPreviousEntityCnt();
    }

    protected void updateReportPayload(ObjectNode report, Table diffReport) {
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            String path = diffReport.getExtracts().get(0).getPath();
            Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path);
            ObjectNode newItem = (ObjectNode) om
                    .readTree(records.next().get(InterfaceName.ConsolidateReport.name()).toString());

            JsonNode entitiesSummaryNode = report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
            if (entitiesSummaryNode == null) {
                entitiesSummaryNode = report.putObject(ReportPurpose.ENTITIES_SUMMARY.getKey());
            }
            JsonNode entityNode = entitiesSummaryNode.get(entity.name());
            if (entityNode == null) {
                entityNode = ((ObjectNode) entitiesSummaryNode).putObject(entity.name());
            }
            JsonNode consolidateSummaryNode = entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            if (consolidateSummaryNode == null) {
                consolidateSummaryNode = ((ObjectNode) entityNode)
                        .putObject(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
            }
            ((ObjectNode) consolidateSummaryNode).setAll((ObjectNode) newItem.get(entity.name()));

            long updateCnt = 0;
            if (entityNode.has(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey())
                    && entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).has(ReportConstants.UPDATE)) {
                updateCnt = entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey())
                        .get(ReportConstants.UPDATE).asLong();
            }
            updateEntityValueMapInContext(entity, UPDATED_RECORDS, updateCnt, Long.class);
            log.info(String.format("Save updated count %d for entity %s to workflow context", updateCnt, entity));

            long newCnt = 0;
            if (entityNode.has(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey())
                    && entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).has(ReportConstants.NEW)) {
                newCnt = entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey()).get(ReportConstants.NEW)
                        .asLong();
            }
            updateEntityValueMapInContext(entity, NEW_RECORDS, newCnt, Long.class);
            log.info(String.format("Save new count %d for entity %s to workflow context", newCnt, entity));
        } catch (Exception e) {
            throw new RuntimeException("Fail to update report payload", e);
        }
    }

    private void markPreviousEntityCnt() {
        DataCollectionStatus detail = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (detail == null) {
            updateEntityValueMapInContext(entity, EXISTING_RECORDS, 0L, Long.class);
            log.error("Fail to find data collection status in workflow context, set previous count as 0 for entity "
                    + entity);
            return;
        }

        Long previousCnt = null;
        switch (entity) {
        case Account:
            previousCnt = detail.getAccountCount();
            break;
        case Contact:
            previousCnt = detail.getContactCount();
            break;
        case Transaction:
            previousCnt = detail.getTransactionCount();
            break;
        default:
            break;
        }

        if (previousCnt == null) {
            previousCnt = 0L;
            log.info(String.format("Fail to find previous count for entity %s in data collection status, set as 0",
                    entity));
        }
        updateEntityValueMapInContext(entity, EXISTING_RECORDS, previousCnt, Long.class);
        log.info(String.format("Save previous count %d for entity %s to workflow context", previousCnt, entity));
    }

    void bumpEntityMatchStagingVersion() {
        List<EntityMatchEnvironment> updatedEnvs = //
                getListObjectFromContext(NEW_ENTITY_MATCH_ENVS, EntityMatchEnvironment.class);
        if (CollectionUtils.isEmpty(updatedEnvs) || !updatedEnvs.contains(EntityMatchEnvironment.STAGING)) {
            BumpVersionRequest request = new BumpVersionRequest();
            request.setTenant(new Tenant(customerSpace.toString()));
            List<EntityMatchEnvironment> environments = Collections.singletonList(EntityMatchEnvironment.STAGING);
            request.setEnvironments(environments);
            log.info("Bump up entity match version for environments = {}", environments);
            BumpVersionResponse response = matchProxy.bumpVersion(request);
            log.info("Current entity match versions = {}", response.getVersions());
            if (updatedEnvs == null) {
                updatedEnvs = new ArrayList<>();
            }
            updatedEnvs.addAll(environments);
            putObjectInContext(NEW_ENTITY_MATCH_ENVS, updatedEnvs);
        }
    }

    Set<String> getTableColumnNames(String... tableNames) {
        // TODO add a batch retrieve API to optimize this
        return Arrays.stream(tableNames) //
                .flatMap(tableName -> metadataProxy //
                        .getTableColumns(customerSpace.toString(), tableName) //
                        .stream() //
                        .map(ColumnMetadata::getAttrName)) //
                .collect(Collectors.toSet());
    }

    private void setScalingMultiplier(List<Table> inputTables) {
        double sizeInGb = 0.0;
        for (Table table : inputTables) {
            sizeInGb += ScalingUtils.getTableSizeInGb(yarnConfiguration, table);
        }
        scalingMultiplier = ScalingUtils.getMultiplier(sizeInGb);
        log.info("Set scalingMultiplier=" + scalingMultiplier //
                + " base on the sum of input table sizes=" + sizeInGb + " gb.");
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        if (request == null) {
            return null;
        } else {
            return transformationProxy.getWorkflowConf(customerSpace.toString(), request, configuration.getPodId());
        }
    }

    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(entity, key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    protected abstract PipelineTransformationRequest getConsolidateRequest();

    protected void mergeInputSchema(String resultTableName) {
        Table resultTable = metadataProxy.getTable(customerSpace.toString(), resultTableName);
        if (resultTable != null && CollectionUtils.isNotEmpty(inputTableNames)) {
            log.info("Update the schema of " + resultTableName + " using input tables' schema.");
            Map<String, Attribute> inputAttrs = new HashMap<>();
            inputTableNames.forEach(tblName -> addAttrsToMap(inputAttrs, tblName));
            updateAttrs(resultTable, inputAttrs);
            metadataProxy.updateTable(customerSpace.toString(), resultTableName, resultTable);
        }
    }

    protected void updateAttrs(Table tgtTable, Map<String, Attribute> srcAttrs) {
        List<Attribute> newAttrs = tgtTable.getAttributes().stream() //
                .map(attr -> srcAttrs.getOrDefault(attr.getName(), attr)).collect(Collectors.toList());
        tgtTable.setAttributes(newAttrs);
    }

    protected void addAttrsToMap(Map<String, Attribute> attrsMap, String tableName) {
        if (StringUtils.isNotBlank(tableName)) {
            Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
            if (table != null) {
                table.getAttributes().forEach(attr -> attrsMap.putIfAbsent(attr.getName(), attr));
            }
        }
    }

    protected String getConvertBatchStoreTableName() {
        Map<String, String> rematchTables = getObjectFromContext(REMATCH_TABLE_NAME, Map.class);
        return (rematchTables != null) && rematchTables.get(configuration.getMainEntity().name()) != null ?
                rematchTables.get(configuration.getMainEntity().name()) : null;
    }

}
