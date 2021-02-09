package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_LDC_DUNS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_EXTRACT_EMBEDDED_ENTITY;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.STAGING;

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
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ConsolidateDataTransformerConfig.ConsolidateDataTxfmrConfigBuilder;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ConsolidateReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ExtractEmbeddedEntityTableConfig;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
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
    protected CDLAttrConfigProxy cdlAttrConfigProxy;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;
    protected BusinessEntity entity;
    protected TableRoleInCollection systemBatchStore;
    protected TableRoleInCollection batchStore;
    protected String batchStoreTablePrefix;
    protected String mergedBatchStoreName;
    protected String systemBatchStoreTablePrefix;
    protected String diffTablePrefix;
    protected String diffReportTablePrefix;
    protected String batchStorePrimaryKey;
    protected List<Table> inputTables = new ArrayList<>();
    protected List<String> rematchInputTableNames = new ArrayList<>();
    protected List<String> inputTableNames = new ArrayList<>();
    protected Table masterTable;
    protected Map<String, String> tableTemplateMap;
    protected List<String> templatesInOrder = new ArrayList<>();
    // A set of columns to filter out, which will be generated later during match
    protected List<String> columnsToFilterOut = new ArrayList<>();
    protected boolean hasSystemBatch;
    Map<BusinessEntity, Boolean> softDeleteEntities;
    Map<BusinessEntity, Boolean> hardDeleteEntities;
    @Inject
    private MatchProxy matchProxy;

    @Value("${cdl.pa.use.directplus}")
    private boolean useDirectPlus;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        generateDiffReport();
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "NPathComplexity" })
    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        entity = configuration.getMainEntity();
        batchStore = entity.getBatchStore();
        if (batchStore != null) {
            batchStoreTablePrefix = entity.name();
            batchStorePrimaryKey = batchStore.getPrimaryKey();
            mergedBatchStoreName = batchStore.name() + "_Merged";
        }
        systemBatchStore = entity.getSystemBatchStore();
        if (systemBatchStore != null) {
            systemBatchStoreTablePrefix = systemBatchStore.name();
        }
        diffTablePrefix = entity.name() + "_Diff";
        diffReportTablePrefix = entity.name() + "_DiffReport";

        // Need to filter out LatticeAccountId and CDLCreatedTemplate
        // as they will be regenerated later on during match
        columnsToFilterOut.add(InterfaceName.LatticeAccountId.name());
        columnsToFilterOut.add(InterfaceName.CDLCreatedTemplate.name());

        if (hasKeyInContext(PERFORM_SOFT_DELETE)) {
            softDeleteEntities = getMapObjectFromContext(PERFORM_SOFT_DELETE, BusinessEntity.class, Boolean.class);
        } else {
            softDeleteEntities = Collections.emptyMap();
        }

        if (hasKeyInContext(PERFORM_HARD_DELETE)) {
            hardDeleteEntities = getMapObjectFromContext(PERFORM_HARD_DELETE, BusinessEntity.class, Boolean.class);
        } else {
            hardDeleteEntities = Collections.emptyMap();
        }

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
        tableTemplateMap = getMapObjectFromContext(CONSOLIDATE_INPUT_TEMPLATES, String.class, String.class);
        hasSystemBatch = systemBatchStore != null && configuration.isEntityMatchEnabled();
        log.info("Has Batch System=" + hasSystemBatch);
        if (hasSystemBatch) {
            Map<BusinessEntity, List> templates = getMapObjectFromContext(CONSOLIDATE_TEMPLATES_IN_ORDER,
                    BusinessEntity.class, List.class);
            if (MapUtils.isNotEmpty(templates)) {
                templatesInOrder = JsonUtils.convertList(templates.get(entity), String.class);
            }
            log.info("Entity=" + entity.name() + " templates in order=" + String.join(",", templatesInOrder));
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

    TransformationStepConfig concatTablesAndSave(String joinKey, String targetTablePrefix) {
        TransformationStepConfig step = dedupAndConcatTables(joinKey, false, inputTableNames);
        setTargetTable(step, targetTablePrefix);
        return step;
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

    TransformationStepConfig concatImports(String targetTablePrefix, String[][] cloneSrcFlds, String[][] renameSrcFlds,
            List<String> convertedRematchTableNames, int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_MERGE_IMPORTS);
        if (inputStep == -1) {
            inputTableNames.forEach(tblName -> addBaseTables(step, tblName));
        } else {
            step.setInputSteps(Collections.singletonList(inputStep));
        }

        if (CollectionUtils.isNotEmpty(convertedRematchTableNames)) {
            convertedRematchTableNames.forEach(tableName -> {
                addBaseTables(step, tableName);
            });
        }

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(false);
        config.setJoinKey(null);
        config.setAddTimestamps(false);
        config.setCloneSrcFields(cloneSrcFlds);
        config.setRenameSrcFields(renameSrcFlds);
        setupTemplates(config, inputStep, convertedRematchTableNames);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        if (StringUtils.isNotBlank(targetTablePrefix)) {
            setTargetTable(step, targetTablePrefix);
        }

        return step;
    }

    private void setupTemplates(MergeImportsConfig config, int inputStep, List<String> convertedRematchTableNames) {
        log.info("inputStep {}, convertedRematchTableNames {}, hasSystemBatch {}", inputStep,
                convertedRematchTableNames, hasSystemBatch);
        if (!hasSystemBatch) {
            return;
        }
        List<String> templates = new ArrayList<>();
        if (CollectionUtils.isEmpty(convertedRematchTableNames)) { // if only regular imports
            inputTableNames.forEach(t -> {
                String templateName = tableTemplateMap.get(t);
                log.info("inputTable={}, templateName={}", t, templateName);
                templates.add(templateName);
                if (StringUtils.isEmpty(templateName)) {
                    log.warn("Template for table {} is empty or null", t);
                }
            });
        } else { // if has rematch fake imports
            if (inputStep != -1) { // for case when both regular imports and fake imports are present
                // set up template placeholder for merged regular imports
                templates.add(SystemBatchTemplateName.PLACEHOLDER.name());
            }

            // add templates for rematch imports
            Map<String, List<String>> rematchTables = getTypedObjectFromContext(REMATCH_TABLE_NAMES,
                    new TypeReference<Map<String, List<String>>>() {
                    });
            if (MapUtils.isNotEmpty(rematchTables)) {
                List<String> tables = rematchTables.get(entity.name());
                tables.forEach(table -> {
                    String templateName = tableTemplateMap.get(table);
                    log.info("rematchTable={}, templateName={}", table, templateName);
                    templates.add(templateName);
                    if (StringUtils.isEmpty(templateName)) {
                        log.warn("Template for table {} is empty or null", table);
                    }
                });
            }
        }

        config.setTemplates(templates);
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
        config.setExcludeAttrs(Collections.singletonList(ATTR_LDC_DUNS));
        if (hasSystemBatch) {
            config.setHasSystem(true);
        }
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
            String targetTableNamePrefix, ETLEngineLoad engineLoad, List<String> convertSystemBatchStoreTableNames,
            int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        if (inputStep == -1) {
            inputTableNames.forEach(tblName -> addBaseTables(step, tblName));
        } else {
            step.setInputSteps(Collections.singletonList(inputStep));
        }
        if (CollectionUtils.isNotEmpty(convertSystemBatchStoreTableNames)) {
            convertSystemBatchStoreTableNames.forEach(tableName -> {
                addBaseTables(step, tableName);
            });
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
        matchInput.setApplicationId(getApplicationId());
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(false);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setReturnAnonymousWhenUnmatched(true); // only affect lookup mode entity match
        matchInput.setUseDirectPlus(useDirectPlus);
        matchInput.setSplitsPerBlock(cascadingPartitions * 10);
        return matchInput;
    }

    void setRematchVersions(MatchInput matchInput) {
        if (Boolean.TRUE.equals(getObjectFromContext(FULL_REMATCH_PA, Boolean.class))) {
            Integer servingVersion = getObjectFromContext(ENTITY_MATCH_REMATCH_SERVING_VERSION, Integer.class);
            Integer stagingVersion = getObjectFromContext(ENTITY_MATCH_REMATCH_STAGING_VERSION, Integer.class);
            matchInput.setEntityMatchVersionMap(ImmutableMap.of(STAGING, stagingVersion, SERVING, servingVersion));
            log.info("Setting entity match versions for rematch. versions = {}", matchInput.getEntityMatchVersionMap());
        }
    }

    // input: streamId -> tablePrefix
    // return: streamId -> table name
    protected Map<String, String> getFullTablenames(Map<String, String> tablePrefixes) {
        if (MapUtils.isEmpty(tablePrefixes)) {
            return Collections.emptyMap();
        }

        return tablePrefixes.entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), TableUtils.getFullTableName(entry.getValue(), pipelineVersion))) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
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
    protected ConsolidateDataTransformerConfig getConsolidateDataTxfmrConfig(boolean dedupeSource,
            boolean addTimettamps, boolean mergeOnly) {
        ConsolidateDataTxfmrConfigBuilder builder = new ConsolidateDataTxfmrConfigBuilder();
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
            setConsolidateSummary(report, diffReport);
        } catch (Exception e) {
            throw new RuntimeException("Fail to update report payload", e);
        }
    }

    private JsonNode setConsolidateSummary(ObjectNode report, Table diffReport) throws JsonProcessingException {
        ObjectMapper om = JsonUtils.getObjectMapper();
        String path = PathUtils.toAvroGlob(diffReport.getExtracts().get(0).getPath());
        Iterator<GenericRecord> records = AvroUtils.iterateAvroFiles(yarnConfiguration, path);
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
        return consolidateSummaryNode;
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
        if (CollectionUtils.isEmpty(updatedEnvs) || !updatedEnvs.contains(STAGING)) {
            BumpVersionRequest request = new BumpVersionRequest();
            request.setTenant(new Tenant(customerSpace.toString()));
            List<EntityMatchEnvironment> environments = Collections.singletonList(STAGING);
            request.setEnvironments(environments);

            // bump up and reserve version for rematch
            reserveStagingVersionForRematch(request);

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

    void reserveStagingVersionForRematch(@NotNull BumpVersionRequest request) {
        if (Boolean.TRUE.equals(getObjectFromContext(FULL_REMATCH_PA, Boolean.class))) {
            Preconditions.checkArgument( //
                    Collections.singletonList(STAGING).equals(request.getEnvironments()), //
                    String.format(
                            "Entity match environment list in bump version request should only contains staging. Envs = %s",
                            request.getEnvironments()));
            log.info("Bump up staging version and reserve for Rematch mode");
            BumpVersionResponse response = matchProxy.bumpVersion(request);
            Preconditions.checkNotNull(response);
            Preconditions.checkNotNull(response.getVersions());
            Preconditions.checkNotNull(response.getVersions().get(STAGING));
            log.info("Reserved staging version for Rematch mode = {}", response.getVersions().get(STAGING));
            putObjectInContext(ENTITY_MATCH_REMATCH_STAGING_VERSION, response.getVersions().get(STAGING));
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
        if (resultTable != null && (CollectionUtils.isNotEmpty(inputTableNames)
                || CollectionUtils.isNotEmpty(rematchInputTableNames))) {
            log.info("Update the schema of {} using input tables' ({}) and rematch input tables' ({}) schema.",
                    resultTableName, inputTableNames, rematchInputTableNames);
            Map<String, Attribute> inputAttrs = new HashMap<>();
            inputTableNames.forEach(tblName -> addAttrsToMap(inputAttrs, tblName));
            if (rematchInputTableNames != null) {
                rematchInputTableNames.forEach(tblName -> addAttrsToMap(inputAttrs, tblName));
            }
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

    protected List<String> getConvertedRematchTableNames() {
        TypeReference<Map<String, List<String>>> ref = new TypeReference<Map<String, List<String>>>() {
        };
        Map<String, List<String>> rematchTables = getTypedObjectFromContext(DELETED_TABLE_NAMES, ref);
        if (rematchTables == null) {
            rematchTables = getTypedObjectFromContext(REMATCH_TABLE_NAMES, ref);
        }
        log.info("rematchTables is : {}", JsonUtils.serialize(rematchTables));
        return (rematchTables != null) && rematchTables.get(configuration.getMainEntity().name()) != null
                ? rematchTables.get(configuration.getMainEntity().name())
                : null;
    }

    protected boolean hasNoImportAndNoBatchStore() {
        if (CollectionUtils.isNotEmpty(inputTableNames)) {
            return false;
        }
        List<String> convertRematchTableNames = getConvertedRematchTableNames();
        return CollectionUtils.isEmpty(convertRematchTableNames);
    }

    protected boolean isHasLegacyDelete(BusinessEntity entity) {
        Set<Action> legacyDeleteAction;
        switch (entity) {
        case Account:
            legacyDeleteAction = getSetObjectFromContext(ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
            if (CollectionUtils.isNotEmpty(legacyDeleteAction)) {
                log.info("legacyDeleteAction is {}", JsonUtils.serialize(legacyDeleteAction));
                return true;
            }
            break;
        case Contact:
            legacyDeleteAction = getSetObjectFromContext(CONTACT_LEGACY_DELTE_BYUOLOAD_ACTIONS, Action.class);
            if (CollectionUtils.isNotEmpty(legacyDeleteAction)) {
                log.info("legacyDeleteAction is {}", JsonUtils.serialize(legacyDeleteAction));
                return true;
            }
            break;
        case Transaction:
            Map<BusinessEntity, Set> actionMap = getMapObjectFromContext(LEGACY_DELETE_BYDATERANGE_ACTIONS,
                    BusinessEntity.class, Set.class);
            Map<CleanupOperationType, Set> legacyDeleteByUploadMap = getMapObjectFromContext(
                    TRANSACTION_LEGACY_DELTE_BYUOLOAD_ACTIONS, CleanupOperationType.class, Set.class);
            if ((actionMap != null && !actionMap.isEmpty())
                    || (legacyDeleteByUploadMap != null && !legacyDeleteByUploadMap.isEmpty())) {
                log.info("already has Transaction legacyDeleteActions");
                return true;
            }
            break;
        default:
            log.info("Entity type {} doesn't support delete", entity.name());
            break;
        }
        return false;
    }

    /**
     * Add transformation steps to extract target entities created by matching
     * activity stream
     *
     * @param extracts
     *            a list that generated steps will be added to
     * @param extractSteps
     *            a list that generated step indices will be added to
     * @param entity
     *            target entity
     * @param newEntityTableCtxKey
     *            context key to a map of entity -> new entities table names
     * @param extractNewEntitiesStepFactory
     *            factory of extract embedded entity config generation function
     */
    protected void addEmbeddedEntitiesFromActivityStream(@NotNull List<TransformationStepConfig> extracts,
            @NotNull List<Integer> extractSteps, @NotNull BusinessEntity entity, @NotNull String newEntityTableCtxKey,
            @NotNull BiFunction<String, String, TransformationStepConfig> extractNewEntitiesStepFactory) {
        if (!hasKeyInContext(newEntityTableCtxKey)) {
            return;
        }
        Map<String, String> newEntityTablesFromStream = getMapObjectFromContext(newEntityTableCtxKey, String.class,
                String.class);
        Map<String, String> streamMatchTables = getMapObjectFromContext(ENTITY_MATCH_STREAM_TARGETTABLE, String.class,
                String.class);
        if (MapUtils.isEmpty(newEntityTablesFromStream)) {
            return;
        }
        for (Map.Entry<String, String> newEntityEntry : newEntityTablesFromStream.entrySet()) {
            String streamId = newEntityEntry.getKey();
            String newEntityTableName = newEntityEntry.getValue();
            String streamMatchTableName = streamMatchTables.get(streamId);
            extracts.add(extractNewEntitiesStepFactory.apply(newEntityTableName, streamMatchTableName));
            extractSteps.add(extractSteps.size());

            log.info("Extracting new {} from table {} with matched stream table {} for stream {}", entity.name(),
                    newEntityTableName, streamMatchTableName, streamId);
        }
    }

    /**
     * Higher order function that return another function which create
     * transformation config to extract target embedded entity
     *
     * @param entity
     *            target entity
     * @param entityIdField
     *            primary ID column for this entity
     * @param systemIds
     *            all match IDs that should be extracted from the matched input
     *            table
     * @return function that generate extract embedded entity transformation config
     */
    protected BiFunction<String, String, TransformationStepConfig> getExtractNewEntitiesStepFactory(
            @NotNull BusinessEntity entity, @NotNull InterfaceName entityIdField, @NotNull List<String> systemIds) {
        return (newEntityTable, matchTargetTable) -> {
            TransformationStepConfig step = new TransformationStepConfig();
            step.setTransformer(TRANSFORMER_EXTRACT_EMBEDDED_ENTITY);
            addBaseTables(step, newEntityTable);
            addBaseTables(step, matchTargetTable);
            ExtractEmbeddedEntityTableConfig config = new ExtractEmbeddedEntityTableConfig();
            config.setEntity(entity.name());
            config.setFilterByEntity(true);
            config.setEntityIdFld(entityIdField.name());
            // SystemIds which don't exist in match target table are ignored in
            // dataflow
            config.setSystemIdFlds(systemIds);
            if (hasSystemBatch) {
                config.setTemplate(SystemBatchTemplateName.Embedded.name());
            }
            step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
            return step;
        };
    }

    protected double getTableSize(@NotNull String tableCtxKey) {
        Table tableSummary = getTableSummaryFromKey(customerSpace.toString(), tableCtxKey);
        return tableSummary == null ? 0 : ScalingUtils.getTableSizeInGb(yarnConfiguration, tableSummary);
    }

    protected double sumTableSizeFromMapCtx(@NotNull String mapCtxKey) {
        if (!hasKeyInContext(mapCtxKey)) {
            return 0.0;
        }

        double totalSize = 0.0;
        Map<String, String> tables = getMapObjectFromContext(mapCtxKey, String.class, String.class);
        if (MapUtils.isEmpty(tables)) {
            return totalSize;
        }

        List<Table> summaries = getTableSummaries(customerSpace.toString(), new ArrayList<>(tables.values()));
        for (Table summary : summaries) {
            if (summary == null) {
                continue;
            }
            totalSize += ScalingUtils.getTableSizeInGb(yarnConfiguration, summary);
        }
        return totalSize;
    }

}
