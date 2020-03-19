package com.latticeengines.admin.tenant.batonadapter.dcp;

import java.util.Collections;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.DefaultConfigOverwriter;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.cdl.CDLComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component("dcpComponent")
public class DCPComponent extends LatticeComponent {
    public static final String componentName = "DCP";

    @Inject
    private TenantService tenantService;

    @Inject
    private CDLComponent cdlComponent;

    @Value("${admin.dcp.dryrun}")
    private boolean dryrun;

    @Inject
    private DefaultConfigOverwriter overwriter;

    private LatticeComponentInstaller installer = new DCPInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new DCPUpgrader();
    private CustomerSpaceServiceDestroyer destroyer = new DCPDestroyer();


    @PostConstruct
    public void setDependenciesAndProducts() {
        dependencies = Collections.singleton(cdlComponent);
    }

    @Override
    public boolean doRegistration() {
        if (uploadSchema) {
            String defaultJson = "dcp_default.json";
            String metadataJson = "dcp_metadata.json";
            uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
            overwriter.overwriteDefaultConfigInPLS();
        }

        return dryrun;
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        installer.setDryrun(false);
        ((DCPInstaller)installer).setTenantService(tenantService);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public CustomerSpaceServiceDestroyer getDestroyer() {
        ((DCPDestroyer) destroyer).setTenantService(tenantService);
        return destroyer;
    }

    @Override
    public String getVersionString() {
        return null;
    }

    @Override
    public Set<LatticeProduct> getAssociatedProducts() {
        return Collections.singleton(LatticeProduct.DCP);
    }

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }
}
