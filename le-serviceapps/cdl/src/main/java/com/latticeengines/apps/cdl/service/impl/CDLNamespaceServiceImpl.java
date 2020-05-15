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
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

@Service("cdlNamespaceService")
public class CDLNamespaceServiceImpl implements CDLNamespaceService {

    private static final Logger log = LoggerFactory.getLogger(CDLNamespaceServiceImpl.class);

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public void setMultiTenantContext(String tenantId) {
        String fullId = CustomerSpace.parse(tenantId).toString();
        String tenantIdInContext = MultiTenantContext.getShortTenantId();
        if (StringUtils.isBlank(tenantIdInContext)
                || !CustomerSpace.parse(tenantIdInContext).toString().equals(fullId)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(fullId);
            if (tenant == null) {
                throw new IllegalArgumentException("Cannot find tenant with id " + tenantId);
            }
            MultiTenantContext.setTenant(tenant);
        }
    }

    @Override
    public <T extends Serializable> Namespace3<String, T, String> prependTenantIdAndAttributeSet(Namespace2<T, String> namespace2) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return Namespace.as(tenantId, namespace2.getCoord1(), namespace2.getCoord2());
    }

    // -> tenantId, tableName
    @Override
    public Namespace2<String, String> resolveTableRole(TableRoleInCollection role, DataCollection.Version version) {
        String tenantId = MultiTenantContext.getShortTenantId();
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
    public boolean hasTableRole(TableRoleInCollection role, DataCollection.Version version) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        List<String> names = dataCollectionService.getTableNames(customerSpace, "", role, version);
        return CollectionUtils.isNotEmpty(names);
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
    public Namespace1<String> resolveDataCloudVersion(DataCollection.Version version) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        if (version == null) {
            version = dataCollectionService.getActiveVersion(customerSpace);
        }
        DataCollectionStatus status = dataCollectionService.getOrCreateDataCollectionStatus(customerSpace, version);
        String dcBuildNumber = status.getDataCloudBuildNumber();
        String dcVersion;
        log.info("Data cloud build number = " + dcBuildNumber);
        if (StringUtils.isBlank(dcBuildNumber) || !dcBuildNumber.matches("^\\d+.*")) {
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
