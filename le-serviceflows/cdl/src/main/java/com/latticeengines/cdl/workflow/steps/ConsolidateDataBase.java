package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateDataBaseConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
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
        Table newMasterTable = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion));
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        if (newMasterTable != null && !BusinessEntity.Transaction.equals(entity)) {
            dataCollectionProxy.upsertTable(customerSpace.toString(), newMasterTable.getName(), batchStore,
                    activeVersion);
        }
        if (isBucketing()) {
            Table redshiftTable = metadataProxy.getTable(customerSpace.toString(),
                    TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion));
            if (redshiftTable == null) {
                throw new RuntimeException("Diff table has not been created.");
            }
            Map<BusinessEntity, Table> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                    BusinessEntity.class, Table.class);
            if (entityTableMap == null) {
                entityTableMap = new HashMap<>();
            }
            entityTableMap.put(entity, redshiftTable);
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
            Table newRecordsTable = metadataProxy.getTable(customerSpace.toString(),
                    TableUtils.getFullTableName(newRecordsTablePrefix, pipelineVersion));
            ObjectNode json = JsonUtils.createObjectNode();
            json.put(getBusinessEntity().getBatchStore().name() + "_New",
                    newRecordsTable.getExtracts().get(0).getProcessedRecords());
            report(ReportPurpose.CONSOLIDATE_NEW_RECORDS_COUNT_SUMMARY, UUID.randomUUID().toString(), json.toString());
            metadataProxy.deleteTable(customerSpace.toString(), newRecordsTable.getName());
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
        step.setConfiguration(getConsolidateDataConfig(true));
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
        step.setConfiguration(getConsolidateDataConfig(false));

        targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(batchStoreTablePrefix);
        targetTable.setPrimaryKey(batchStorePrimaryKey);
        step.setTargetTable(targetTable);
        return step;
    }

    protected TransformationStepConfig diff(int mergeStep, int upsertMasterStep) {
        if (!isBucketing()) {
            return null;
        }
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(mergeStep, upsertMasterStep));
        step.setTransformer("consolidateDeltaTransformer");
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        setupConfig(config);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    protected TransformationStepConfig sortDiff(int diffStep) {
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
        config.setPartitions(100);
        String sortingKey = servingStorePrimaryKey;
        if (!servingStoreSortKeys.isEmpty()) {
            sortingKey = servingStoreSortKeys.get(0);
        }
        config.setSortingField(sortingKey);
        config.setCompressResult(true);
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

    protected String getConsolidateDataConfig(boolean isDedupeSource) {
        ConsolidateDataTransformerConfig config = new ConsolidateDataTransformerConfig();
        config.setSrcIdField(srcIdField);
        config.setMasterIdField(batchStorePrimaryKey);
        setupConfig(config);
        config.setDedupeSource(isDedupeSource);
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
