package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class BaseSingleEntityProfileStep<T extends BaseProcessEntityStepConfiguration>
        extends BaseTransformWrapperStep<T> {

    protected CustomerSpace customerSpace;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected String profileTablePrefix;
    protected String statsTablePrefix;
    protected String servingStoreTablePrefix;
    protected String servingStoreSortKey;

    protected BusinessEntity entity;
    protected Table masterTable;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();
        return generateWorkflowConf();
    }

    @Override
    protected void onPostTransformationCompleted() {
        String profileTableName = TableUtils.getFullTableName(profileTablePrefix, pipelineVersion);
        String statsTableName = TableUtils.getFullTableName(statsTablePrefix, pipelineVersion);
        String servingStoreTableName = TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion);

        upsertProfileTable(profileTableName, profileTableRole());

        Table servingStoreTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), servingStoreTableName);
        enrichTableSchema(servingStoreTable);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), servingStoreTableName, servingStoreTable);

        updateEntityValueMapInContext(TABLE_GOING_TO_REDSHIFT, servingStoreTableName, String.class);
        updateEntityValueMapInContext(APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);

        updateEntityValueMapInContext(SERVING_STORE_IN_STATS, servingStoreTableName, String.class);
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
    }

    private void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        entity = configuration.getMainEntity();

        TableRoleInCollection servingStore = entity.getServingStore();
        profileTablePrefix = entity.name() + "Profile";
        statsTablePrefix = entity.name() + "Stats";
        servingStoreTablePrefix = servingStore.name();
        servingStoreSortKey = servingStore.getPrimaryKey().name();

        masterTable = dataCollectionProxy.getTable(customerSpace.toString(), entity.getBatchStore(), inactive);
        if (masterTable == null) {
            throw new IllegalStateException("Cannot find the master table in default collection");
        }
    }

    private TransformationWorkflowConfiguration generateWorkflowConf() {
        PipelineTransformationRequest request = getTransformRequest();
        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    private void upsertProfileTable(String profileTableName, TableRoleInCollection profileRole) {
        String customerSpace = configuration.getCustomerSpace().toString();
        Table profileTable = metadataProxy.getTable(customerSpace, profileTableName);
        if (profileTable == null) {
            throw new RuntimeException(
                    "Failed to find profile table " + profileTableName + " in customer " + customerSpace);
        }
        DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpace);
        dataCollectionProxy.upsertTable(customerSpace, profileTableName, profileRole, inactiveVersion);
        profileTable = dataCollectionProxy.getTable(customerSpace, profileRole, inactiveVersion);
        if (profileTable == null) {
            throw new IllegalStateException("Cannot find the upserted " + profileRole + " table in data collection.");
        }
    }

    protected void enrichTableSchema(Table servingStoreTable) {
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

    protected abstract TableRoleInCollection profileTableRole();

    protected abstract PipelineTransformationRequest getTransformRequest();

}
