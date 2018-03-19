package com.latticeengines.admin.tenant.batonadapter.cdl;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

/**
 * This is a dummy installer for functional tests, the true installer resides on
 * PLS server
 */
public class CDLInstaller extends LatticeComponentInstaller {

    public CDLInstaller() {
        super(CDLComponent.componentName);
    }

    private TenantService tenantService;

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        if (tenantService == null) {
            throw new IllegalStateException("CDL Installer is not wired with a TenantService.");
        }

        return configDir;
    }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }
}
