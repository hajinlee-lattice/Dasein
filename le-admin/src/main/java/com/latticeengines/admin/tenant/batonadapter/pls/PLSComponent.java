package com.latticeengines.admin.tenant.batonadapter.pls;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class PLSComponent extends LatticeComponent {
    public static final String componentName = "PLS";

    @Value("${admin.pls.dryrun}")
    private boolean dryrun;

    @Override
    public String getName() { return componentName; }

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
        String defaultJson = "pls_default.json";
        String metadataJson = "pls_metadata.json";
        uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
        return false;
    }


}
