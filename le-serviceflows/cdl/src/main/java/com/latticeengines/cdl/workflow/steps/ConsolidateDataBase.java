package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CONSOLIDATE_RETAIN;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

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

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateRetainFieldConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateDataBaseConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class ConsolidateDataBase<T extends ConsolidateDataBaseConfiguration>
        extends BaseTransformWrapperStep<T> {

    protected static final Logger log = LoggerFactory.getLogger(ConsolidateDataBase.class);

    protected static final String CREATION_DATE = "CREATION_DATE";

    protected CustomerSpace customerSpace = null;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Autowired
    protected DataCollectionProxy dataCollectionProxy;

    protected List<String> inputTableNames = new ArrayList<>();
    protected String inputMasterTableName;
    protected Table masterTable;
    protected String profileTableName;

    protected String servingStoreTablePrefix;
    protected String servingStorePrimaryKey;
    protected List<String> servingStoreSortKeys;
    protected String batchStoreTablePrefix;
    protected String batchStorePrimaryKey;
    protected List<String> batchStoreSortKeys;

    protected BusinessEntity entity;
    protected TableRoleInCollection batchStore;
    protected TableRoleInCollection servingStore;

    protected String srcIdField;

    protected String newRecordsTablePrefix = "newRecordsTablePrefix";

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        onPostTransformationCompleted(true);
    }

    protected void onPostTransformationCompleted(boolean isPublish) {
        Table newMasterTable = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion));
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        if (isPublish) {
            if (entity.getBatchStore() != null) {
                if (newMasterTable == null) {
                    throw new IllegalStateException("Did not generate new master table for " + entity);
                }
                dataCollectionProxy.upsertTable(customerSpace.toString(), newMasterTable.getName(), batchStore,
                        activeVersion);
            }
        }

        if (isBucketing() && isPublish) {
            String redshiftTableName = TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion);
            Table redshiftTable = metadataProxy.getTable(customerSpace.toString(), redshiftTableName);
            if (redshiftTable == null) {
                throw new RuntimeException("Diff table has not been created.");
            }
            Map<BusinessEntity, String> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                    BusinessEntity.class, String.class);
            if (entityTableMap == null) {
                entityTableMap = new HashMap<>();
            }
            entityTableMap.put(entity, redshiftTableName);
            putObjectInContext(TABLE_GOING_TO_REDSHIFT, entityTableMap);

            Map<BusinessEntity, Boolean> appendTableMap = getMapObjectFromContext(APPEND_TO_REDSHIFT_TABLE,
                    BusinessEntity.class, Boolean.class);
            if (appendTableMap == null) {
                appendTableMap = new HashMap<>();
            }
            appendTableMap.put(entity, true);
            putObjectInContext(APPEND_TO_REDSHIFT_TABLE, appendTableMap);
        }

        if (BusinessEntity.Account.equals(getBusinessEntity())) {
            metadataProxy.deleteTable(customerSpace.toString(),
                    TableUtils.getFullTableName(newRecordsTablePrefix, pipelineVersion));
        }

        Table diffReport = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(getReportTablePrefix(), pipelineVersion));
        if (diffReport != null) {
            Report report = retrieveReport(customerSpace, ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY);
            String reportName = report != null ? report.getName() : UUID.randomUUID().toString();
            String reportPayload = updateReportPayload(report, diffReport);
            report(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY, reportName, reportPayload);
            metadataProxy.deleteTable(customerSpace.toString(), diffReport.getName());
        } else {
            log.warn("Didn't find ConsolidationSummaryReport table for entity " + getBusinessEntity());
        }
    }

    private String updateReportPayload(Report report, Table diffReport) {
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode json = report != null && StringUtils.isNotBlank(report.getJson().getPayload())
                    ? (ObjectNode) om.readTree(report.getJson().getPayload()) : om.createObjectNode();
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
        entity = getBusinessEntity();
        batchStore = entity.getBatchStore();
        if (batchStore != null) {
            batchStoreTablePrefix = entity.name();
            batchStorePrimaryKey = batchStore.getPrimaryKey().name();
            batchStoreSortKeys = batchStore.getForeignKeysAsStringList();
        }
        servingStore = entity.getServingStore();
        if (servingStore != null) {
            servingStoreTablePrefix = servingStore.name();
            servingStorePrimaryKey = servingStore.getPrimaryKey().name();
            servingStoreSortKeys = servingStore.getForeignKeysAsStringList();
        }
        @SuppressWarnings("rawtypes")
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);

        List<DataFeedImport> imports = JsonUtils.convertList(entityImportsMap.get(entity), DataFeedImport.class);
        List<Table> inputTables = imports.stream().map(DataFeedImport::getDataTable).collect(Collectors.toList());

        if (inputTables == null || inputTables.isEmpty()) {
            throw new RuntimeException("There is no input tables to consolidate.");
        }
        Collections.reverse(inputTables);
        inputTables.sort(Comparator.comparing((Table t) -> t.getLastModifiedKey() == null ? -1
                : t.getLastModifiedKey().getLastModifiedTimestamp() == null ? -1
                        : t.getLastModifiedKey().getLastModifiedTimestamp())
                .reversed());
        for (Table table : inputTables) {
            inputTableNames.add(table.getName());
        }

        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore);
        if (masterTable == null || masterTable.getExtracts().isEmpty()) {
            log.info("There has been no master table for this data collection. Creating a new one");
        } else {
            inputMasterTableName = masterTable.getName();
        }
        log.info("Set inputMasterTableName=" + inputMasterTableName);

        if (isBucketing()) {
            findProfileTable();
        }
    }

    protected void findProfileTable() {
    }

    protected TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
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
            targetTable.setNamePrefix(getMergeTableName());
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
        config.setSrcIdField(srcIdField);
        setupConfig(config);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig sortDiff(int diffStep, int partitions) {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(diffStep));
        step.setTransformer(TRANSFORMER_SORTER);

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        targetTable.setPrimaryKey(servingStorePrimaryKey);
        targetTable.setExpandBucketedAttrs(true);
        step.setTargetTable(targetTable);

        SorterConfig config = new SorterConfig();
        config.setPartitions(partitions);
        String sortingKey = servingStorePrimaryKey;
        if (!servingStoreSortKeys.isEmpty()) {
            sortingKey = servingStoreSortKeys.get(0);
        }
        config.setSortingField(sortingKey);
        config.setCompressResult(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    protected TransformationStepConfig retainFields(int previousStep, boolean useTargetTable) {
        return retainFields(previousStep, useTargetTable, servingStore);
    }

    protected TransformationStepConfig retainFields(int previousStep, boolean useTargetTable,
            TableRoleInCollection role) {

        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(previousStep));
        step.setTransformer(TRANSFORMER_CONSOLIDATE_RETAIN);

        if (useTargetTable) {
            TargetTable targetTable = new TargetTable();
            targetTable.setCustomerSpace(customerSpace);
            targetTable.setNamePrefix(servingStoreTablePrefix);
            targetTable.setPrimaryKey(servingStorePrimaryKey);
            targetTable.setExpandBucketedAttrs(true);
            step.setTargetTable(targetTable);
        }
        ConsolidateRetainFieldConfig config = new ConsolidateRetainFieldConfig();
        Table servingTable = dataCollectionProxy.getTable(customerSpace.toString(), role);
        if (servingTable != null) {
            List<String> fieldsToRetain = AvroUtils.getSchemaFields(yarnConfiguration,
                    servingTable.getExtracts().get(0).getPath());
            config.setFieldsToRetain(fieldsToRetain);
        }
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig bucket(int inputStep, boolean heavyEngine) {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerProfile";
        SourceTable sourceTable = new SourceTable(profileTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setTransformer(TRANSFORMER_BUCKETER);
        String confStr = heavyEngine ? emptyStepConfig(heavyEngineConfig()) : emptyStepConfig(lightEngineConfig());
        step.setConfiguration(confStr);
        return step;
    }

    protected TransformationStepConfig reportDiff(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setTransformer("ConsolidateReporter");
        ConsolidateReportConfig config = new ConsolidateReportConfig();
        config.setEntity(getBusinessEntity());
        String configStr = appendEngineConf(config, lightEngineConfig());
        step.setConfiguration(configStr);
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(getReportTablePrefix());
        step.setTargetTable(targetTable);
        return step;

    }

    private String getReportTablePrefix() {
        return getBusinessEntity().name() + "ReportTablePrefix";
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

    public boolean isBucketing() {
        return configuration.isBucketing();
    }

    protected String getConsolidateDataConfig(boolean isDedupeSource, boolean addTimestamps) {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        config.setMasterIdField(batchStorePrimaryKey);
        setupConfig(config);
        config.setDedupeSource(isDedupeSource);
        config.setAddTimestamps(addTimestamps);
        return appendEngineConf(config, lightEngineConfig());
    }

    protected String getMergeTableName() {
        return batchStore.name() + "_Merged";
    }

    protected abstract void setupConfig(ConsolidateDataTransformerConfig config);

    public abstract PipelineTransformationRequest getConsolidateRequest();

    public BusinessEntity getBusinessEntity() {
        return configuration.getBusinessEntity();
    }

}
