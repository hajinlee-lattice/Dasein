package com.latticeengines.admin.entitymgr;

import java.util.List;

import com.latticeengines.documentdb.entity.TenantConfigEntity;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.security.TenantStatus;

public interface TenantConfigEntityMgr {
    boolean createTenant(String contractId, String tenantId, ContractProperties contractProperties,
            TenantProperties tenantProperties,
            CustomerSpaceProperties customerSpaceProperties, FeatureFlagValueMap featureFlags,
            SpaceConfiguration spaceConfiguration);

    List<TenantConfigEntity> getTenants();

    List<TenantConfigEntity> getTenantsInReader();

    boolean deleteTenant(String contractId, String tenantId);

    TenantConfigEntity getTenant(String contractId, String tenantId);

    TenantConfigEntity getTenantInReader(String contractId, String tenantId);

    void updateTenantStatus(Long pid, TenantStatus status);


}
