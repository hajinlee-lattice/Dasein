package com.latticeengines.admin.tenant.batonadapter.globalauth;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public class GlobalAuthInstaller extends LatticeComponentInstaller {

    public GlobalAuthInstaller() { super(GlobalAuthComponent.componentName); }

    @Override
    public DocumentDirectory installCore(
            CustomerSpace space, String serviceName, int dataVersion,
            CustomerSpaceProperties properties, DocumentDirectory autoGenDocDir) {
        return null;
    }
}
