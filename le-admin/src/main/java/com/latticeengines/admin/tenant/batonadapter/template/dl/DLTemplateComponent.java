package com.latticeengines.admin.tenant.batonadapter.template.dl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.dynamicopts.impl.TemplateProvider;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component
public class DLTemplateComponent extends LatticeComponent {

    private LatticeComponentInstaller installer = new DLTemplateInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new DLTemplateUpgrader();
    public static final String componentName = "DLTemplate";

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TemplateProvider templateProvider;

    @Autowired
    private VisiDBDLComponent visiDBDLComponent;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Value("${admin.dl.tpl.dryrun}")
    private boolean dryrun;

    @PostConstruct
    public void setDependenciesAndProducts() {
        dependencies = Collections.singleton(visiDBDLComponent);
        Set<LatticeProduct> productSet = new HashSet<LatticeProduct>();
        productSet.add(LatticeProduct.LPA);
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
        installer.setDryrun(dryrun);
        ((DLTemplateInstaller) installer).setTenantService(tenantService);
        ((DLTemplateInstaller) installer).setTemplateProvider(templateProvider);
        ((DLTemplateInstaller) installer).setDataloaderService(dataLoaderService);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public String getVersionString() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean doRegistration() {
        String defaultJson = "dl_tpl_default.json";
        String metadataJson = "dl_tpl_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }
}
