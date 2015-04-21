package com.latticeengines.admin.tenant.batonadapter.bardjams;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class BardJamsComponent extends LatticeComponent {

    @Autowired
    private BardJamsEntityMgr bardJamsEntityMgr;

    @Value("${admin.bardjams.timeout}")
    private int timeout;

    private CustomerSpaceServiceInstaller installer = new BardJamsInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new BardJamsUpgrader();
    public static final String componentName = "BardJams";

    // register bootstrapper upon instantiation
    public BardJamsComponent() { register(); }

    @Override
    public boolean doRegistration() {
        String defaultJson = "bardjams_default.json";
        String metadataJson = "bardjams_metadata.json";
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

        ((BardJamsInstaller) installer).setBardJamsEntityMgr(bardJamsEntityMgr);
        ((BardJamsInstaller) installer).setTimeout(timeout);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public String getVersionString() {
        return "1.0";
    }

}
