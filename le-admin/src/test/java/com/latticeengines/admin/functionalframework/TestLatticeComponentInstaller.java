package com.latticeengines.admin.functionalframework;

import java.util.Map;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class TestLatticeComponentInstaller extends LatticeComponentInstaller {

    public TestLatticeComponentInstaller() { super(TestLatticeComponent.componentName); }

    @Override
    public DocumentDirectory installCore(
            CustomerSpace space, String serviceName, int dataVersion,
            Map<String, String> properties, DocumentDirectory autoGenDocDir) {
        return null;
    }
}
