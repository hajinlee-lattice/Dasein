package com.latticeengines.admin.tenant.batonadapter.vdb;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

public class VisiDBComponent extends LatticeComponent {
    
    private CustomerSpaceServiceInstaller installer = new VisiDBInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new VisiDBUpgrader();

    @Override
    public String getName() {
        return "VDB";
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public String getVersionString() {
        // TODO Auto-generated method stub
        return null;
    }

}
