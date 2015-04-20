package com.latticeengines.admin.tenant.batonadapter.globalauth;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class GlobalAuthComponent extends LatticeComponent {
    
    private CustomerSpaceServiceInstaller installer = new GlobalAuthInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new GlobalAuthUpgrader();
    public static final String componentName = "GlobalAuth";

    @Override
    public boolean doRegistration() { return false; }

    @Override
    public String getName() { return componentName; }

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
