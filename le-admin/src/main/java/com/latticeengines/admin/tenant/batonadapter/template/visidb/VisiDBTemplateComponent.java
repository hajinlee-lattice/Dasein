package com.latticeengines.admin.tenant.batonadapter.template.visidb;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class VisiDBTemplateComponent extends LatticeComponent {
    
    private LatticeComponentInstaller installer = new VisiDBTemplateInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new VisiDBTemplateUpgrader();
    public static final String componentName = "VisiDBTemplate";

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
        String defaultJson = "visidb_tpl_default.json";
        String metadataJson = "visidb_tpl_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }


}
