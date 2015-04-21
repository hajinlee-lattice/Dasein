package com.latticeengines.admin.tenant.batonadapter.vdb;

import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;

public class VisiDBInstaller extends LatticeComponentInstaller {

    public VisiDBInstaller() { super(VisiDBComponent.componentName); }

    @Override
    public void installCore(
            CustomerSpace space, String serviceName, int dataVersion,
            CustomerSpaceInfo spaceInfo, DocumentDirectory autoGenDocDir) {
    }
}
