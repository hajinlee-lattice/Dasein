package com.latticeengines.admin.tenant.batonadapter.pls;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

public class PLSComponent extends LatticeComponent {
    
    private CustomerSpaceServiceInstaller installer = new PLSInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new PLSUpgrader();

    @Override
    public String getName() {
        return "PLS";
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

}
