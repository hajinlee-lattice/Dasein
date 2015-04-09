package com.latticeengines.admin.tenant.batonadapter.template;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

public class TemplateComponent extends LatticeComponent {
    
    private CustomerSpaceServiceInstaller installer = new TemplateInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new TemplateUpgrader();

    @Override
    public String getName() {
        return "TPL";
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
    
    @Override
    public boolean doRegistration() {
        return false;
    }


}
