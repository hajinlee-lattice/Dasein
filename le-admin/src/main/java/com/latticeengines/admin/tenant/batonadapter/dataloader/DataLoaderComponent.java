package com.latticeengines.admin.tenant.batonadapter.dataloader;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

public class DataLoaderComponent extends LatticeComponent {
    
    private CustomerSpaceServiceInstaller installer = new DataLoaderInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new DataLoaderUpgrader();
    
    @Override
    public String getName() {
        return "DataLoader";
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
