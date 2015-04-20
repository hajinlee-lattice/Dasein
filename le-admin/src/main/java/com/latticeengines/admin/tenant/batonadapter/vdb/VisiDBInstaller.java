package com.latticeengines.admin.tenant.batonadapter.vdb;

import java.util.Map;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class VisiDBInstaller extends LatticeComponentInstaller {

    public VisiDBInstaller() { super(VisiDBComponent.componentName); }

    @Override
    public DocumentDirectory installCore(
            CustomerSpace space, String serviceName, int dataVersion,
            Map<String, String> properties, DocumentDirectory autoGenDocDir) {
        return null;
    }
}
