package com.latticeengines.admin.tenant.batonadapter.pls;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class PLSComponent extends LatticeComponent {
    public static final String componentName = "PLS";
    
    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LatticeComponentInstaller getInstaller() {
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
        String defaultJson = "pls_default.json";
        String metadataJson = "pls_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }


}
