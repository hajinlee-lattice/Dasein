package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
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

    protected CustomerSpace customerSpace = null;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Autowired
    protected DataCollectionProxy dataCollectionProxy;

    protected List<String> inputTableNames = new ArrayList<>();
    protected Boolean isActive = false;
    protected String inputMasterTableName;
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
        dataCollectionProxy.upsertTable(customerSpace.toString(), newMasterTable.getName(), batchStore, activeVersion);
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
            json.put(getBusinessEntity().name() + "_New", newRecordsTable.getExtracts().get(0).getProcessedRecords());
            report(ReportPurpose.CONSOLIDATE_NEW_RECORDS_COUNT_SUMMARY, UUID.randomUUID().toString(), json.toString());
            metadataProxy.deleteTable(customerSpace.toString(), newRecordsTable.getName());
        }
    }

    protected void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        entity = getBusinessEntity();
        batchStore = entity.getBatchStore();
        batchStoreTablePrefix = entity.name();
        batchStorePrimaryKey = batchStore.getPrimaryKey().name();
        batchStoreSortKeys = batchStore.getForeignKeysAsStringList();
        servingStore = entity.getServingStore();
        servingStoreTablePrefix = servingStore.name();
        servingStorePrimaryKey = servingStore.getPrimaryKey().name();
        servingStoreSortKeys = servingStore.getForeignKeysAsStringList();
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

        Table masterTable = dataCollectionProxy.getTable(customerSpace.toString(), batchStore);
        if (masterTable == null || masterTable.getExtracts().isEmpty()) {
            log.info("There has been no master table for this data collection. Creating a new one");
        } else {
            inputMasterTableName = masterTable.getName();
        }
        log.info("Set inputMasterTableName=" + inputMasterTableName);

        isActive = getObjectFromContext(IS_ACTIVE, Boolean.class);
        if (isBucketing()) {
            Table profileTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.Profile);
            profileTableName = profileTable.getName();
            log.info("Set profileTableName=" + profileTableName);
        }
    }

    protected TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getConsolidateRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    public abstract boolean isBucketing();

    public abstract PipelineTransformationRequest getConsolidateRequest();

    public BusinessEntity getBusinessEntity() {
        return configuration.getBusinessEntity();
    }

}
