package com.latticeengines.admin.tenant.batonadapter.metadata;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class MetadataInstaller extends LatticeComponentInstaller {

    public MetadataInstaller() {
        super(MetadataComponent.componentName);
    }

    @Override
    protected DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        return configDir;
    }

}
