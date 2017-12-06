package com.latticeengines.cdl.workflow.steps.merge;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class BaseSingleEntityMergeImports<T extends BaseProcessEntityStepConfiguration>
        extends BaseTransformWrapperStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseSingleEntityMergeImports.class);

    protected CustomerSpace customerSpace;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected BusinessEntity entity;
    protected TableRoleInCollection batchStore;
    protected String batchStoreTablePrefix;
    protected String mergedBatchStoreName;
    private String diffTablePrefix;
    protected String diffReportTablePrefix;
    protected String batchStorePrimaryKey;
    protected String newRecordsTablePrefix;
    protected List<String> batchStoreSortKeys;

    protected List<String> inputTableNames = new ArrayList<>();
    protected String inputMasterTableName;
    protected Table masterTable;

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        Table newMasterTable = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion));
        if (entity.getBatchStore() != null) {
            if (newMasterTable == null) {
                throw new IllegalStateException("Did not generate new master table for " + entity);
            }
            dataCollectionProxy.upsertTable(customerSpace.toString(), newMasterTable.getName(), batchStore,
                    inactive);
        }

        String diffTableName = TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
        updateEntityValueMapInContext(ENTITY_DIFF_TABLES, diffTableName, String.class);

        Table diffReport = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(diffReportTablePrefix, pipelineVersion));
        if (diffReport != null) {
            Report report = retrieveReport(customerSpace, ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY);
            String reportName = report != null ? report.getName() : UUID.randomUUID().toString();
            String reportPayload = updateReportPayload(report, diffReport);
            report(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY, reportName, reportPayload);
            metadataProxy.deleteTable(customerSpace.toString(), diffReport.getName());
        } else {
            log.warn("Didn't find ConsolidationSummaryReport table for entity " + entity);
        }
    }

    private String updateReportPayload(Report report, Table diffReport) {
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode json = report != null && StringUtils.isNotBlank(report.getJson().getPayload())
                    ? (ObjectNode) om.readTree(report.getJson().getPayload())
                    : om.createObjectNode();
            String path = diffReport.getExtracts().get(0).getPath();
            Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path);
            ObjectNode newItem = (ObjectNode) om
                    .readTree(records.next().get(InterfaceName.ConsolidateReport.name()).toString());
            json.setAll(newItem);
            return json.toString();
        } catch (Exception e) {
            throw new RuntimeException("Fail to update report payload", e);
        }
    }

    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

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
        newRecordsTablePrefix = entity.name() + "_NewRecords";

        @SuppressWarnings("rawtypes")
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);

        List<DataFeedImport> imports = JsonUtils.convertList(entityImportsMap.get(entity), DataFeedImport.class);
        List<Table> inputTables = imports.stream().map(DataFeedImport::getDataTable).collect(Collectors.toList());

        if (inputTables == null || inputTables.isEmpty()) {
            throw new RuntimeException("There is no input tables to consolidate.");
        }
        inputTables.sort(Comparator.comparing((Table t) -> t.getLastModifiedKey() == null ? -1
                : t.getLastModifiedKey().getLastModifiedTimestamp() == null ? -1
                : t.getLastModifiedKey().getLastModifiedTimestamp())
                .reversed());
        for (Table table : inputTables) {
            inputTableNames.add(table.getName());
        }

        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore, active);
        if (masterTable == null || masterTable.getExtracts().isEmpty()) {
            log.info("There has been no master table for this data collection. Creating a new one");
        } else {
            inputMasterTableName = masterTable.getName();
        }
        log.info("Set inputMasterTableName=" + inputMasterTableName);
    }

    protected TransformationStepConfig mergeInputs(boolean useTargetTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSources = inputTableNames;
        step.setBaseSources(baseSources);

        Map<String, SourceTable> baseTables = new HashMap<>();
        for (String inputTableName : inputTableNames) {
            baseTables.put(inputTableName, new SourceTable(inputTableName, customerSpace));
        }
        step.setBaseTables(baseTables);
        step.setTransformer("consolidateDataTransformer");
        step.setConfiguration(getConsolidateDataConfig(true, true));
        if (useTargetTable) {
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(mergedBatchStoreName);
            step.setTargetTable(targetTable);
        }
        return step;
    }

    protected TransformationStepConfig mergeMaster(int mergeStep) {
        TargetTable targetTable;
        TransformationStepConfig step = new TransformationStepConfig();
        setupMasterTable(step);
        step.setInputSteps(Collections.singletonList(mergeStep));
        step.setTransformer("consolidateDataTransformer");
        step.setConfiguration(getConsolidateDataConfig(false, false));

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(batchStoreTablePrefix);
        targetTable.setPrimaryKey(batchStorePrimaryKey);
        step.setTargetTable(targetTable);
        return step;
    }

    protected TransformationStepConfig diff(int mergeStep, int upsertMasterStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(mergeStep, upsertMasterStep));
        step.setTransformer("consolidateDeltaTransformer");
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(InterfaceName.Id.name());
        config.setMasterIdField(batchStorePrimaryKey);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(diffTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

    protected TransformationStepConfig reportDiff(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setTransformer("ConsolidateReporter");
        ConsolidateReportConfig config = new ConsolidateReportConfig();
        config.setEntity(entity);
        String configStr = appendEngineConf(config, lightEngineConfig());
        step.setConfiguration(configStr);
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(diffReportTablePrefix);
        step.setTargetTable(targetTable);
        return step;

    }

    protected void setupMasterTable(TransformationStepConfig step) {
        List<String> baseSources;
        Map<String, SourceTable> baseTables;
        if (StringUtils.isNotBlank(inputMasterTableName)) {
            Table masterTable = metadataProxy.getTable(customerSpace.toString(), inputMasterTableName);
            if (masterTable != null && !masterTable.getExtracts().isEmpty()) {
                baseSources = Collections.singletonList(inputMasterTableName);
                baseTables = new HashMap<>();
                SourceTable sourceMasterTable = new SourceTable(inputMasterTableName, customerSpace);
                baseTables.put(inputMasterTableName, sourceMasterTable);
                step.setBaseSources(baseSources);
                step.setBaseTables(baseTables);
            }
        }
    }

    protected String getConsolidateDataConfig(boolean isDedupeSource, boolean addTimestamps) {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(InterfaceName.Id.name());
        config.setMasterIdField(batchStorePrimaryKey);
        config.setDedupeSource(isDedupeSource);
        config.setAddTimestamps(addTimestamps);
        return appendEngineConf(config, lightEngineConfig());
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    private <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(entity, key, value, clz);
    }

    private <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    protected abstract PipelineTransformationRequest getConsolidateRequest();

}
