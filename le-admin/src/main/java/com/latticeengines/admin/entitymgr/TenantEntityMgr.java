package com.latticeengines.admin.entitymgr;

import java.util.Collection;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface TenantEntityMgr {

    boolean createTenant(String contractId, String tenantId, ContractInfo contractInfo, TenantInfo tenantInfo,
            CustomerSpaceInfo customerSpaceInfo);

    Collection<TenantDocument> getTenants(String contractId);

    boolean deleteTenant(String contractId, String tenantId);

    TenantDocument getTenant(String contractId, String tenantId);

    BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName);

    SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName);

    SpaceConfiguration getDefaultSpaceConfig();
}
