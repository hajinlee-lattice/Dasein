package com.latticeengines.admin.tenant.batonadapter.template.dl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class DLTemplateComponent extends LatticeComponent {
    
    private LatticeComponentInstaller installer = new DLTemplateInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new DLTemplateUpgrader();
    public static final String componentName = "DLTemplate";

    @Value("${admin.tpl.dryrun}")
    private boolean dryrun;

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        installer.setDryrun(dryrun);
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
        String defaultJson = "dl_tpl_default.json";
        String metadataJson = "dl_tpl_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }


}
