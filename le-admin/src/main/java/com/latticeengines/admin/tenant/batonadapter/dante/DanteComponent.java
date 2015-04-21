package com.latticeengines.admin.tenant.batonadapter.dante;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class DanteComponent extends LatticeComponent {
    public static final String componentName = "Dante";

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
        return null;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return null;
    }

    @Override
    public String getVersionString() {
        return null;
    }
    
    @Override
    public boolean doRegistration() {
        String defaultJson = "dante_default.json";
        String metadataJson = "dante_metadata.json";
        uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
        return false;
    }


}
