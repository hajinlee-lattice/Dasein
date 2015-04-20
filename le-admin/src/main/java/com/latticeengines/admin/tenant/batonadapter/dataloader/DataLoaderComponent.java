package com.latticeengines.admin.tenant.batonadapter.dataloader;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class DataLoaderComponent extends LatticeComponent {
    
    private CustomerSpaceServiceInstaller installer = new DataLoaderInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new DataLoaderUpgrader();
    public static final String componentName = "DataLoader";

    @Override
    public boolean doRegistration() {
        String defaultJson = "dl_default.json";
        String metadataJson = "dl_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }
    
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

}
