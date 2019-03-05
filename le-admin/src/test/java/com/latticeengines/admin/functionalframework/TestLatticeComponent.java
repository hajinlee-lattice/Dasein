package com.latticeengines.admin.functionalframework;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class TestLatticeComponent extends LatticeComponent {

    private CustomerSpaceServiceUpgrader upgrader = new TestLatticeComponentUpgrader();
    private CustomerSpaceServiceDestroyer destroyer = new TestLatticeComponentDestroyer();
    public String componentName = "TestComponent";
    private boolean dryrun = true;

    public TestLatticeComponent() {
    }

    public TestLatticeComponent(boolean dryrun) {
        this.dryrun = dryrun;
    }

    @PostConstruct
    public void postConstruct() {
        if (!batonService.getRegisteredServices().contains(getName())) {
            register();
        }
    }

    @Override
    public Set<LatticeProduct> getAssociatedProducts() {
        return new HashSet<>(Arrays.asList(LatticeProduct.LPA, LatticeProduct.LPA3, LatticeProduct.PD,
                LatticeProduct.CG));
    }

    @Override
    public boolean doRegistration() {
        String defaultJson = "testcomponent_default.json";
        String metadataJson = "testcomponent_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public void setName(String name) {
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        LatticeComponentInstaller installer = new TestLatticeComponentInstaller(getName());
        installer.setDryrun(dryrun);
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
        return "1.0";
    }

    public void setDependencies(Collection<? extends LatticeComponent> dependencies) {
        this.dependencies = dependencies;
    }

}
