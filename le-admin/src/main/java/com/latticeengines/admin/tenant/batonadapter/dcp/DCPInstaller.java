package com.latticeengines.admin.tenant.batonadapter.dcp;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

/**
 * This is a dummy installer for functional tests, the true installer resides on
 * DCP server
 */
public class DCPInstaller extends LatticeComponentInstaller {

    private TenantService tenantService;

    public DCPInstaller() {
        super(DCPComponent.componentName);
    }

    @Override
    protected DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
                                                                   int dataVersion, DocumentDirectory configDir) {
        if (tenantService == null) {
            throw new IllegalStateException("DCP Installer is not wired with a TenantService.");
        }
        return configDir;
    }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }
}
