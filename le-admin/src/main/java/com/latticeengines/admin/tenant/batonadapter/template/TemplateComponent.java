package com.latticeengines.admin.tenant.batonadapter.template;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class TemplateComponent extends LatticeComponent {
    
    private CustomerSpaceServiceInstaller installer = new TemplateInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new TemplateUpgrader();
    public static final String componentName = "Template";

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
        String defaultJson = "tpl_default.json";
        String metadataJson = "tpl_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }


}
