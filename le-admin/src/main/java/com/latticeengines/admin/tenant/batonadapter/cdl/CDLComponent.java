package com.latticeengines.admin.tenant.batonadapter.cdl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.DefaultConfigOverwriter;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component("cdlComponent")
public class CDLComponent extends LatticeComponent {
    public static final String componentName = "CDL";

    @Inject
    private PLSComponent plsComponent;

    @Value("${admin.cdl.dryrun}")
    private boolean dryrun;

    @Inject
    private DefaultConfigOverwriter overwriter;

    @Inject
    private TenantService tenantService;

    @PostConstruct
    public void setDependenciesAndProducts() {
        dependencies = Collections.singleton(plsComponent);
    }

    private LatticeComponentInstaller installer = new CDLInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new CDLUpgrader();
    private CustomerSpaceServiceDestroyer destroyer = new CDLDestroyer();

    @Override
    public Set<LatticeProduct> getAssociatedProducts() {
        return new HashSet<>(Collections.singletonList(LatticeProduct.CG));
    }

    @Override
    public boolean hasV2Api() {
        return true;
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
        ((CDLInstaller) installer).setTenantService(tenantService);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public CustomerSpaceServiceDestroyer getDestroyer() {
        ((CDLDestroyer) destroyer).setTenantService(tenantService);
        return destroyer;
    }

    @Override
    public String getVersionString() {
        return null;
    }

    @Override
    public boolean doRegistration() {
        if (uploadSchema) {
            String defaultJson = "cdl_default.json";
            String metadataJson = "cdl_metadata.json";
            uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
            overwriter.overwriteDefaultConfigInPLS();
        }

        return dryrun;
    }
}
