package com.latticeengines.admin.tenant.batonadapter.metadata;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class MetadataComponent extends LatticeComponent {

    @Value("${admin.metadata.dryrun}")
    private boolean dryrun;

    @Autowired
    private PLSComponent plsComponent;

    private LatticeComponentInstaller installer = new MetadataInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new MetadataUpgrader();

    public static final String componentName = "Metadata";

    @PostConstruct
    public void setDependencies() {
        dependencies = Collections.singleton(plsComponent);
        Set<LatticeProduct> productSet = new HashSet<LatticeProduct>();
        productSet.add(LatticeProduct.PD);
        super.setAssociatedProducts(productSet);
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
    public boolean doRegistration() {
        String defaultJson = "metadata_default.json";
        String metadataJson = "metadata_metadata.json";
        uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
        return dryrun;
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        installer.setDryrun(dryrun);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public String getVersionString() {
        return null;
    }

}
