package com.latticeengines.admin.tenant.batonadapter.pls;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class PLSDestroyer implements CustomerSpaceServiceDestroyer {

    private TenantService tenantService;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        if (tenantService == null) {
            throw new IllegalStateException("PLS Destroyer is not wired with a TenantService.");
        } else {
            throw new RuntimeException("An intend exception for the purpose of testing destroyer.");
        }
    }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }
}
