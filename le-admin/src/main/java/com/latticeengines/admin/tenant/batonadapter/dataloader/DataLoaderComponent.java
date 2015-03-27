package com.latticeengines.admin.tenant.batonadapter.dataloader;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.config.bootstrap.Installer;
import com.latticeengines.camille.exposed.config.bootstrap.Upgrader;

@Component("dataLoaderComponent")
public class DataLoaderComponent extends LatticeComponent {
    
    private Installer installer = new DataLoaderInstaller();
    private Upgrader upgrader = new DataLoaderUpgrader();

    @Override
    public String getName() {
        return "DataLoader";
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Installer getInstaller() {
        return installer;
    }

    @Override
    public Upgrader getUpgrader() {
        return upgrader;
    }

}
