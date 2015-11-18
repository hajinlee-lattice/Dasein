package com.latticeengines.admin.tenant.batonadapter.eai;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class EaiInstaller extends LatticeComponentInstaller {

    public EaiInstaller() {
        super(EaiComponent.componentName);
    }

    @Override
    protected DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        return configDir;
    }

}
