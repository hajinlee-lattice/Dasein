package com.latticeengines.admin.tenant.batonadapter.globalauth;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.config.bootstrap.Installer;
import com.latticeengines.camille.exposed.config.bootstrap.Upgrader;

@Component("globalAuthComponent")
public class GlobalAuthComponent extends LatticeComponent {
    
    private Installer installer = new GlobalAuthInstaller();
    private Upgrader upgrader = new GlobalAuthUpgrader();

    @Override
    public String getName() {
        return "GlobalAuth";
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
