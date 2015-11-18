package com.latticeengines.admin.tenant.batonadapter.eai;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class EaiComponent extends LatticeComponent {

    @Value("${admin.eai.dryrun}")
    private boolean dryrun;

    private LatticeComponentInstaller installer = new EaiInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new EaiUpgrader();
    public static final String componentName = "Eai";

    @PostConstruct
    public void setProducts() {
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
        String defaultJson = "eai_default.json";
        String metadataJson = "eai_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
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
        return "1.0";
    }

}
