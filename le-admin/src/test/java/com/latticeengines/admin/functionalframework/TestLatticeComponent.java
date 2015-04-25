package com.latticeengines.admin.functionalframework;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class TestLatticeComponent extends LatticeComponent {

    private final LatticeComponentInstaller installer = new TestLatticeComponentInstaller();
    private final CustomerSpaceServiceUpgrader upgrader = new TestLatticeComponentUpgrader();
    public static final String componentName = "TestComponent";

    public TestLatticeComponent() { }

    @Override
    public boolean doRegistration() {
        String defaultJson = "testcomponent_default.json";
        String metadataJson = "testcomponent_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }

    @Override
    public String getName() { return componentName; }

    @Override
    public void setName(String name) { }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() { return installer; }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() { return upgrader; }

    @Override
    public String getVersionString() { return "1.0"; }

}
