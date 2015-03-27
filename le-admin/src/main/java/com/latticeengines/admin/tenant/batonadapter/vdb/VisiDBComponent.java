package com.latticeengines.admin.tenant.batonadapter.vdb;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.config.bootstrap.Installer;
import com.latticeengines.camille.exposed.config.bootstrap.Upgrader;

@Component("visiDBComponent")
public class VisiDBComponent extends LatticeComponent {
    
    private Installer installer = new VisiDBInstaller();
    private Upgrader upgrader = new VisiDBUpgrader();

    @Override
    public String getName() {
        return "VDB";
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
