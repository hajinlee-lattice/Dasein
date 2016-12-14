package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.remote.exposed.service.DataLoaderService;

public class VisiDBDLDestroyer implements CustomerSpaceServiceDestroyer {

    private DataLoaderService dataLoaderService;

    private TenantService tenantService;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        String dmDeployment = space.getTenantId();
        String contractExternalID = space.getContractId();
        if (tenantService == null) {
            return false;
        }
        TenantDocument tenantDoc = tenantService.getTenant(contractExternalID, dmDeployment);
        if (tenantDoc != null) {
            String tenant = dmDeployment;
            String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
            DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenant, "3");
            dataLoaderService.deleteDLTenant(request, dlUrl, true);
        }
        return true;
    }

    public void setDataloaderService(DataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }
}
