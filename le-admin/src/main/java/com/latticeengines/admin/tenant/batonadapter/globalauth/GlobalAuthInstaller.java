package com.latticeengines.admin.tenant.batonadapter.globalauth;

import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class GlobalAuthInstaller extends LatticeComponentInstaller {

    public GlobalAuthInstaller() { super(GlobalAuthComponent.componentName); }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory autoGenDocDir) {
    }
}
