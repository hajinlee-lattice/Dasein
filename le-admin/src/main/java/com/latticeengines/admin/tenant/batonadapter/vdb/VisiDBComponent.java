package com.latticeengines.admin.tenant.batonadapter.vdb;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class VisiDBComponent extends LatticeComponent {
    
    private CustomerSpaceServiceInstaller installer = new VisiDBInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new VisiDBUpgrader();
    public static final String componentName = "VisiDB";

    @Override
    public boolean doRegistration() {
        String defaultJson = "vdb_default.json";
        String metadataJson = "vdb_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }

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
