package com.latticeengines.admin.tenant.batonadapter.pls;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.DefaultConfigOverwriter;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.modeling.ModelingComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component("plsComponent")
public class PLSComponent extends LatticeComponent {
    public static final String componentName = "PLS";

    @Autowired
    private ModelingComponent modelingComponent;

    @PostConstruct
    public void setDependenciesAndProducts() {
        dependencies = Collections.singleton(modelingComponent);
    }

    @Value("${admin.pls.dryrun}")
    private boolean dryrun;

    @Autowired
    private DefaultConfigOverwriter overwriter;

    @Autowired
    private TenantService tenantService;

    private LatticeComponentInstaller installer = new PLSInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new PLSUpgrader();
    private CustomerSpaceServiceDestroyer destroyer = new PLSDestroyer();

    @Override
    public Set<LatticeProduct> getAssociatedProducts() {
        return new HashSet<>(Arrays.asList(LatticeProduct.LPA, LatticeProduct.LPA3, LatticeProduct.PD));
    }

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public boolean hasV2Api() {
        return true;
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
    public CustomerSpaceServiceDestroyer getDestroyer() {
        ((PLSDestroyer) destroyer).setTenantService(tenantService);
        return destroyer;
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
            overwriter.overwriteDefaultConfigInPLS();
        }

        return dryrun;
    }
}
