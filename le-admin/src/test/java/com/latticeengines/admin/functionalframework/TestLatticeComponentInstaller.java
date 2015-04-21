package com.latticeengines.admin.functionalframework;


import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class TestLatticeComponentInstaller extends LatticeComponentInstaller {

    public TestLatticeComponentInstaller() { super(TestLatticeComponent.componentName); }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory autoGenDocDir) {}
}
