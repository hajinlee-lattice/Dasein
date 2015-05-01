package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class VisiDBDLComponent extends LatticeComponent {

    private LatticeComponentInstaller installer = new VisiDBDLInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new VisiDBDLUpgrader();
    public static final String componentName = "VisiDBDL";

    @Value("${admin.vdb.dryrun}")
    private boolean dryrun;

    @Value("${admin.dl.url}")
    private String dlUrl;

    @Override
    public boolean doRegistration() {
        String defaultJson = "vdbdl_default.json";
        String metadataJson = "vdbdl_metadata.json";
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
        installer.setDryrun(dryrun);
        ((VisiDBDLInstaller)installer).setDLUrl(dlUrl);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public String getVersionString() {
        return "2.7";
    }
}
