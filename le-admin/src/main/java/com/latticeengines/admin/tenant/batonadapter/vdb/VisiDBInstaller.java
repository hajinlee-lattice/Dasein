package com.latticeengines.admin.tenant.batonadapter.vdb;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public class VisiDBInstaller extends LatticeComponentInstaller {

    public VisiDBInstaller() { super(VisiDBComponent.componentName); }

    @Override
    public void installCore(
            CustomerSpace space, String serviceName, int dataVersion,
            CustomerSpaceProperties spaceProps, DocumentDirectory autoGenDocDir) {
    }
}
