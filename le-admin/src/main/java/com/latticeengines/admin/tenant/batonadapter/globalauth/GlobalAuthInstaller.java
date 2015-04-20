package com.latticeengines.admin.tenant.batonadapter.globalauth;

import java.util.Map;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class GlobalAuthInstaller extends LatticeComponentInstaller {

    public GlobalAuthInstaller() { super(GlobalAuthComponent.componentName); }

    @Override
    public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion, Map<String, String> properties) {
        return null;
    }
}
