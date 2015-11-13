package com.latticeengines.admin.tenant.batonadapter.pls;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

/**
 * This is a dummy installer for functional tests, the true installer resides on
 * PLS server
 */
public class PLSInstaller extends LatticeComponentInstaller {

    public PLSInstaller() {
        super(PLSComponent.componentName);
    }

    private TenantService tenantService;

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        if (tenantService == null) {
            throw new IllegalStateException("PLS Installer is not wired with a TenantService.");
        } else {
            System.out.println("There are " + String.valueOf(tenantService.getTenants(null).size())
                    + " registered tenants.");
        }

        return configDir;
    }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }
}
