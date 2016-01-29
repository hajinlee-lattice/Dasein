package com.latticeengines.admin.tenant.batonadapter.pls;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.DefaultConfigOverwritter;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component("plsComponent")
public class PLSComponent extends LatticeComponent {
    public static final String componentName = "PLS";

    @Value("${admin.pls.dryrun}")
    private boolean dryrun;

    @Autowired
    private DefaultConfigOverwritter overwritter;

    @Autowired
    private TenantService tenantService;

    private LatticeComponentInstaller installer = new PLSInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new PLSUpgrader();

    @PostConstruct
    public void setProducts() {
        Set<LatticeProduct> productSet = new HashSet<LatticeProduct>();
        productSet.add(LatticeProduct.LPA);
        productSet.add(LatticeProduct.LPA3);
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
    public CustomerSpaceServiceInstaller getInstaller() {
        installer.setDryrun(false);
        ((PLSInstaller) installer).setTenantService(tenantService);
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

    @Override
    public boolean doRegistration() {
        if (uploadSchema) {
            String defaultJson = "pls_default.json";
            String metadataJson = "pls_metadata.json";
            uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
            overwritter.overwriteDefaultConfigInPLS();
        }

        return dryrun;
    }
}
