package com.latticeengines.admin.entitymgr;

import java.util.AbstractMap;
import java.util.List;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface TenantEntityMgr {

    Boolean createTenant(String contractId, String tenantId,
                         ContractInfo contractInfo, TenantInfo tenantInfo, CustomerSpaceInfo customerSpaceInfo);

    List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId);
    
    Boolean deleteTenant(String contractId, String tenantId);

    TenantInfo getTenant(String contractId, String tenantId);
    
    BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName);
    
    SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName);

}
