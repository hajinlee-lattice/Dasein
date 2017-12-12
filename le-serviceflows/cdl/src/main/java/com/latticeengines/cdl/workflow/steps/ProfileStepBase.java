package com.latticeengines.cdl.workflow.steps;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class ProfileStepBase<T extends BaseWrapperStepConfiguration> extends BaseTransformWrapperStep<T> {

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    protected TransformationProxy transformationProxy;

    protected <V> void updateEntityValueMapInContext(String key, V value, Class<V> clz) {
        updateEntityValueMapInContext(getEntity(), key, value, clz);
    }

    protected <V> void updateEntityValueMapInContext(BusinessEntity entity, String key, V value, Class<V> clz) {
        Map<BusinessEntity, V> entityValueMap = getMapObjectFromContext(key, BusinessEntity.class, clz);
        if (entityValueMap == null) {
            entityValueMap = new HashMap<>();
        }
        entityValueMap.put(entity, value);
        putObjectInContext(key, entityValueMap);
    }

    void upsertProfileTable(String profileTableName, TableRoleInCollection profileRole) {
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

    protected abstract BusinessEntity getEntity();

}
