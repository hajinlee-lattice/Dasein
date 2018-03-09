package com.latticeengines.apps.cdl.service.impl;

import java.io.Serializable;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.CDLNamespaceService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Service("cdlNamespaceService")
public class CDLNamespaceServiceImpl implements CDLNamespaceService {

    private static final Logger log = LoggerFactory.getLogger(CDLNamespaceServiceImpl.class);

    @Inject
    private DataCollectionService dataCollectionService;

    @Override
    public <T extends Serializable> Namespace2<String, T> prependTenantId(Namespace1<T> namespace1) {
        String tenantId = MultiTenantContext.getTenantId();
        return Namespace.as(tenantId, namespace1.getCoord1());
    }

    // -> tenantId, tableName
    @Override
    public Namespace2<String, String> resolveTableRole(TableRoleInCollection role, DataCollection.Version version) {
        String tenantId = MultiTenantContext.getTenantId();
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        List<String> names = dataCollectionService.getTableNames(customerSpace, "", role, version);
        String tableName = CollectionUtils.isNotEmpty(names) ? names.get(0) : null;
        if (StringUtils.isBlank(tableName)) {
            throw new IllegalArgumentException(
                    "Cannot find table name for " + role + " at version " + version + " in tenant " + tenantId);
        }
        return Namespace.as(tenantId, tableName);
    }

    @Override
    public Namespace2<String, String> resolveServingStore(BusinessEntity businessEntity, DataCollection.Version version) {
        TableRoleInCollection role = businessEntity.getServingStore();
        if (role == null) {
            throw new IllegalArgumentException("Business Entity " + businessEntity + " does not have a serving store role.");
        }
        return resolveTableRole(role, version);
    }

    @Override
    public Namespace2<String, String> resolveServingStore(Namespace2<BusinessEntity, DataCollection.Version> namespace) {
        BusinessEntity entity = namespace.getCoord1();
        DataCollection.Version version = namespace.getCoord2();
        return resolveServingStore(entity, version);
    }

    @Override
    public Namespace1<String> resolveDataCloudVersion() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        String dcBuildNumber = dataCollectionService.getDataCollection(customerSpace, "").getDataCloudBuildNumber();
        String dcVersion;
        if (StringUtils.isBlank(dcBuildNumber)) {
            log.warn("Tenant " + customerSpace + " does not have a data cloud build number.");
            dcVersion = "";
        } else {
            dcVersion = dcBuildNumber.substring(0, dcBuildNumber.lastIndexOf("."));
        }
        return Namespace.as(dcVersion);
    }

    @Override
    public <T extends Serializable> Namespace2<T, DataCollection.Version> appendActiveVersion(T coord) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataCollection.Version active = dataCollectionService.getActiveVersion(customerSpace);
        return Namespace.as(coord, active);
    }

    @Override
    public <T extends Serializable> Namespace2<T, DataCollection.Version> appendActiveVersion(Namespace1<T> namespace1) {
        return appendActiveVersion(namespace1.getCoord1());
    }

}
