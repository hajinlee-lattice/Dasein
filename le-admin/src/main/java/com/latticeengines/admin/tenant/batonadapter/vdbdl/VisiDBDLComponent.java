package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.dynamicopts.impl.DataStoreProvider;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component("visiDBDLComponent")
public class VisiDBDLComponent extends LatticeComponent {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private DataStoreProvider dataStoreProvider;

    @Autowired
    private ServiceService serviceService;

    @Value("${admin.vdbdl.dryrun}")
    private boolean dryrun;

    @Value("${admin.overwrite.dl.url:DEFAULT}")
    private String dlUrl;

    @Value("${admin.overwrite.dl.url.options:DEFAULT}")
    private String dlUrlOptions;

    @Value("${admin.overwrite.dl.owner:DEFAULT}")
    private String dlOwner;

    @Value("${admin.overwrite.dl.datastore:DEFAULT}")
    private String dlDatastore;

    @Value("${admin.overwrite.vdb.permstore:DEFAULT}")
    private String vdbPermstore;

    @Value("${admin.overwrite.vdb.permstore.options:DEFAULT}")
    private String vdbPermstoreOptions;

    private LatticeComponentInstaller installer = new VisiDBDLInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new VisiDBDLUpgrader();
    public static final String componentName = "VisiDBDL";

    @Override
    public boolean doRegistration() {
        String defaultJson = "vdbdl_default.json";
        String metadataJson = "vdbdl_metadata.json";
        uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
        overwriteDefaultConfigAndSchema();
        return true;
    }

    @Override
    public String getName() { return componentName; }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {
        installer.setDryrun(dryrun);
        ((VisiDBDLInstaller)installer).setTenantService(tenantService);
        ((VisiDBDLInstaller)installer).setDataStoreProvider(dataStoreProvider);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public String getVersionString() {
        return "2.7";
    }


    private void overwriteDefaultConfigAndSchema() {
        if (isToBeOverwritten(dlOwner)) {
            serviceService.patchDefaultConfig(componentName, "/DL/OwnerEmail", dlOwner);
        }

        if (isToBeOverwritten(dlDatastore)) {
            serviceService.patchDefaultConfig(componentName, "/DL/DataStore", dlDatastore);
        }

        if (isToBeOverwritten(vdbPermstore) && isToBeOverwritten(vdbPermstoreOptions)) {
            overwritePermstoreConfig();
        }
    }

    private boolean isToBeOverwritten(String value) {
        return value != null && !value.equals("DEFAULT");
    }

    private void overwritePermstoreConfig() {
        SelectableConfigurationField patch = new SelectableConfigurationField();
        patch.setNode("/VisiDB/PermanentStore");
        patch.setDefaultOption(vdbPermstore);
        patch.setOptions(Arrays.asList(vdbPermstoreOptions.split(",")));
        serviceService.patchDefaultConfigWithOptions(componentName, patch);
    }
}
