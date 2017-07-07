package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateDataBaseConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class ConsolidateDataBase<T extends ConsolidateDataBaseConfiguration>
        extends BaseTransformWrapperStep<T> {

    protected static final Log log = LogFactory.getLog(ConsolidateDataBase.class);

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

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        Table newMasterTable = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion));
        dataCollectionProxy.upsertTable(customerSpace.toString(), newMasterTable.getName(), batchStore);
        if (isBucketing()) {
            Table diffTable = metadataProxy.getTable(customerSpace.toString(),
                    TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion));
            if (diffTable == null) {
                throw new RuntimeException("Diff table has not been created.");
            }
            Map<BusinessEntity, Table> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT,
                    BusinessEntity.class, Table.class);
            if (entityTableMap == null) {
                entityTableMap = new HashMap<>();
            }
            entityTableMap.put(entity, diffTable);
            putObjectInContext(TABLE_GOING_TO_REDSHIFT, entityTableMap);
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
