package com.latticeengines.admin.tenant.batonadapter.datacloud;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;


public class DataCloudInstaller extends LatticeComponentInstaller {

    public DataCloudInstaller() {
        super(DataCloudComponent.componentName);
    }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        return configDir;
    }
}
