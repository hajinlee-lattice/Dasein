package com.latticeengines.admin.functionalframework;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class TestLatticeComponentInstaller extends LatticeComponentInstaller {

    public TestLatticeComponentInstaller() {
        super("TestComponent");
    }

    public TestLatticeComponentInstaller(String componentName) {
        super(componentName);
    }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory autoGenDocDir) {
        throw new RuntimeException("This installation is meant to be failed.");
    }
}
