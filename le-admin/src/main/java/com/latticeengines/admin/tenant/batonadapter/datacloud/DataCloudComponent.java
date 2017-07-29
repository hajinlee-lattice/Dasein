package com.latticeengines.admin.tenant.batonadapter.datacloud;

import java.util.Collections;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class DataCloudComponent extends LatticeComponent {
    public static final String componentName = "DataCloud";

    private LatticeComponentInstaller installer = new DataCloudInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new DataCloudUpgrader();
    private CustomerSpaceServiceDestroyer destroyer = new DataCloudDestroyer();

    @Override
    public Set<LatticeProduct> getAssociatedProducts() {
        return Collections.singleton(LatticeProduct.LPA3);
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
        installer.setDryrun(false);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public CustomerSpaceServiceDestroyer getDestroyer() {
        return destroyer;
    }

    @Override
    public String getVersionString() {
        return null;
    }

    @Override
    public boolean doRegistration() {
        String defaultJson = "datacloud_default.json";
        String metadataJson = "datacloud_metadata.json";
        uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
        // always register this component
        return true;
    }

}
