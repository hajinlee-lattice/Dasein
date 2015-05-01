package com.latticeengines.admin.functionalframework;

import java.util.Collection;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class TestLatticeComponent extends LatticeComponent {

    private CustomerSpaceServiceUpgrader upgrader = new TestLatticeComponentUpgrader();
    public String componentName = "TestComponent";
    public boolean dryrun = true;

    public TestLatticeComponent() { }

    public TestLatticeComponent(boolean dryrun) { this.dryrun = dryrun; }

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
    public CustomerSpaceServiceInstaller getInstaller() {
        LatticeComponentInstaller installer = new TestLatticeComponentInstaller(getName());
        installer.setDryrun(dryrun);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() { return upgrader; }

    @Override
    public String getVersionString() { return "1.0"; }

    public void setDependencies(Collection<? extends LatticeComponent> dependencies) { this.dependencies = dependencies; }

}
