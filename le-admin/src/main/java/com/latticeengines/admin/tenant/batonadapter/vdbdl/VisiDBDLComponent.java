package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.dynamicopts.impl.DataStoreProvider;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.DefaultConfigOverwriter;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component("visiDBDLComponent")
public class VisiDBDLComponent extends LatticeComponent {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private DataStoreProvider dataStoreProvider;

    @Autowired
    private DefaultConfigOverwriter overwriter;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Value("${admin.vdbdl.dryrun}")
    private boolean dryrun;

    private LatticeComponentInstaller installer = new VisiDBDLInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new VisiDBDLUpgrader();
    private CustomerSpaceServiceDestroyer destroyer = new VisiDBDLDestroyer();
    public static final String componentName = "VisiDBDL";

    @Override
    public Set<LatticeProduct> getAssociatedProducts() {
        return new HashSet<>(Arrays.asList(LatticeProduct.LPA));
    }

    @Override
    public boolean doRegistration() {
        if (uploadSchema) {
            String defaultJson = "vdbdl_default.json";
            String metadataJson = "vdbdl_metadata.json";
            uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
            overwriter.overwriteDefaultConfigInVisiDBDL();
        }
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
        installer.setDryrun(dryrun);
        ((VisiDBDLInstaller) installer).setTenantService(tenantService);
        ((VisiDBDLInstaller) installer).setDataStoreProvider(dataStoreProvider);
        ((VisiDBDLInstaller) installer).setDataloaderService(dataLoaderService);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public CustomerSpaceServiceDestroyer getDestroyer() {
        ((VisiDBDLDestroyer) destroyer).setDataloaderService(dataLoaderService);
        ((VisiDBDLDestroyer) destroyer).setTenantService(tenantService);
        return destroyer;
    }

    @Override
    public String getVersionString() {
        return "2.7";
    }
}
