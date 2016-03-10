package com.latticeengines.admin.tenant.batonadapter.modeling;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class ModelingInstaller extends LatticeComponentInstaller {

    public ModelingInstaller() {
        super(ModelingComponent.componentName);
    }

    private TenantService tenantService;

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        if (tenantService == null) {
            throw new IllegalStateException("Modeling Installer is not wired with a TenantService.");
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


