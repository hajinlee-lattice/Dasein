package com.latticeengines.admin.tenant.batonadapter.pls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class PLSComponent extends LatticeComponent {
    public static final String componentName = "PLS";

    @Value("${admin.pls.dryrun}")
    private boolean dryrun;

    @Value("${admin.overwrite.pls.superadmin:DEFAULT}")
    private String superAdmins;

    @Value("${admin.overwrite.pls.latticeadmin:DEFAULT}")
    private String latticeAdmins;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private ServiceService serviceService;

    private LatticeComponentInstaller installer = new PLSInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new PLSUpgrader();

    @Override
    public String getName() { return componentName; }

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
        String defaultJson = "pls_default.json";
        String metadataJson = "pls_metadata.json";
        uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
        overwriteDefaultConfigAndSchema();
        // register dummy installer if is in dryrun mode
        return dryrun;
    }

    private void overwriteDefaultConfigAndSchema() {
        ObjectMapper mapper = new ObjectMapper();
        List<String> adminEmailList = new ArrayList<>();
        if (superAdmins != null && !superAdmins.equals("DEFAULT")) {
            List<String> emailList = new ArrayList<>();
            for (String email: Arrays.asList(superAdmins.split(","))) {
                if (!StringUtils.isEmpty(email)) { emailList.add(email); }
            }
            try {
                serviceService.patchDefaultConfig(componentName, "/SuperAdminEmails",
                        mapper.writeValueAsString(emailList));
                adminEmailList.addAll(emailList);
            } catch (IOException e) {
                // ignore
            }
        }

        if (latticeAdmins != null && !latticeAdmins.equals("DEFAULT")) {
            List<String> emailList = new ArrayList<>();
            for (String email: Arrays.asList(latticeAdmins.split(","))) {
                if (!StringUtils.isEmpty(email)) { emailList.add(email); }
            }
            emailList.removeAll(adminEmailList);
            try {
                serviceService.patchDefaultConfig(componentName, "/LatticeAdminEmails",
                        mapper.writeValueAsString(emailList));
            } catch (IOException e) {
                // ignore
            }
        }
    }

}
