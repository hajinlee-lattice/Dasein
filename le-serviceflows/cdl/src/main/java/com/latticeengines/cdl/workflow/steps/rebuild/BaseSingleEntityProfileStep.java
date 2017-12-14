package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.cdl.workflow.steps.ProfileStepBase;
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

public abstract class BaseSingleEntityProfileStep<T extends BaseProcessEntityStepConfiguration>
        extends ProfileStepBase<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseSingleEntityProfileStep.class);

    protected CustomerSpace customerSpace;
    protected DataCollection.Version active;
    protected DataCollection.Version inactive;

    protected String profileTablePrefix;
    protected String statsTablePrefix;
    protected String servingStoreTablePrefix;
    protected String servingStoreSortKey;

    protected BusinessEntity entity;
    protected Table masterTable;
    protected boolean publishToRedshift = true;

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
        String profileTableName = TableUtils.getFullTableName(profileTablePrefix, pipelineVersion);
        String statsTableName = TableUtils.getFullTableName(statsTablePrefix, pipelineVersion);
        String servingStoreTableName = TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion);

        upsertProfileTable(profileTableName, profileTableRole());

        Table servingStoreTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                servingStoreTableName);
        enrichTableSchema(servingStoreTable);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), servingStoreTableName,
                servingStoreTable);

        if (publishToRedshift) {
            updateEntityValueMapInContext(TABLE_GOING_TO_REDSHIFT, servingStoreTableName, String.class);
            updateEntityValueMapInContext(APPEND_TO_REDSHIFT_TABLE, false, Boolean.class);
        }

        updateEntityValueMapInContext(SERVING_STORE_IN_STATS, servingStoreTableName, String.class);
        updateEntityValueMapInContext(STATS_TABLE_NAMES, statsTableName, String.class);
    }

    protected void initializeConfiguration() {
        customerSpace = CustomerSpace.parse(getObjectFromContext(CUSTOMER_SPACE, String.class));
        active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
        entity = getEntityToBeProfiled();

        TableRoleInCollection servingStore = entity.getServingStore();
        profileTablePrefix = entity.name() + "Profile";
        statsTablePrefix = entity.name() + "Stats";
        servingStoreTablePrefix = servingStore.name();
        servingStoreSortKey = servingStore.getPrimaryKey().name();

        if (entity.getBatchStore() != null) {
            String masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(), entity.getBatchStore(),
                    inactive);
            if (StringUtils.isBlank(masterTableName)) {
                masterTableName = dataCollectionProxy.getTableName(customerSpace.toString(), entity.getBatchStore(), active);
                if (StringUtils.isNotBlank(masterTableName)) {
                    log.info("Found the batch store in active version " + active);
                }
            } else {
                log.info("Found the batch store in inactive version " + inactive);
            }
            masterTable = metadataProxy.getTable(customerSpace.toString(), masterTableName);
            if (masterTable == null) {
                throw new IllegalStateException("Cannot find the master table in default collection");
            }
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

    protected BusinessEntity getEntityToBeProfiled() {
        return configuration.getMainEntity();
    }

    @Override
    protected BusinessEntity getEntity() {
        return getEntityToBeProfiled();
    }

    protected abstract TableRoleInCollection profileTableRole();

    protected abstract PipelineTransformationRequest getTransformRequest();

}
