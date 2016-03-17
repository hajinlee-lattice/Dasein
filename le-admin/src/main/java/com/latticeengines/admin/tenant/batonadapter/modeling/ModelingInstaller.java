package com.latticeengines.admin.tenant.batonadapter.modeling;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class ModelingInstaller extends LatticeComponentInstaller {

    public ModelingInstaller() {
        super(ModelingComponent.componentName);
    }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
        int dataVersion, DocumentDirectory configDir) {

        return configDir;
    }

}

