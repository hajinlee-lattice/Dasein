package com.latticeengines.admin.tenant.batonadapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;

@Component
public class DefaultConfigOverwriter {

    private static final Logger log = LoggerFactory.getLogger(DefaultConfigOverwriter.class);
    private static final String LIST_DELIMITER = ",";

    @Inject
    private ServiceService serviceService;

    @Inject
    private DynamicOptionsService dynamicOptionsService;

    //TODO: (Yintao - May 2018) should explicitly set values in all environments
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

    @Value("${admin.overwrite.vdb.servername:DEFAULT}")
    private String vdbServername;

    @Value("${admin.overwrite.vdb.servername.options:DEFAULT}")
    private String vdbServernameOptions;

    @Value("${admin.overwrite.pls.superadmin:DEFAULT}")
    private String plsSuperAdmins;

    @Value("${admin.overwrite.pls.latticeadmin:DEFAULT}")
    private String plsLatticeAdmins;

    public void overwriteDefaultSpaceConfig() {
        if (isToBeOverwritten(dlUrl) && isToBeOverwritten(dlUrlOptions)) {
            SelectableConfigurationField patch = new SelectableConfigurationField();
            patch.setNode("/DL_Address");
            patch.setDefaultOption(dlUrl);
            patch.setOptions(Arrays.asList(dlUrlOptions.split(LIST_DELIMITER)));
            serviceService.patchDefaultConfigWithOptions(LatticeComponent.spaceConfigNode, patch);
            dynamicOptionsService.updateMutableOptionsProviderSource(LatticeComponent.spaceConfigNode, patch);
        }
    }

    public void overwriteDefaultConfigInPLS() {
        ObjectMapper mapper = new ObjectMapper();
        List<String> adminEmailList = new ArrayList<>();
        if (isToBeOverwritten(plsSuperAdmins)) {
            List<String> emailList = new ArrayList<>();
            for (String email : plsSuperAdmins.split(LIST_DELIMITER)) {
                if (!StringUtils.isEmpty(email))
                    emailList.add(email);
            }
            try {
                serviceService.patchDefaultConfig(PLSComponent.componentName, "/SuperAdminEmails",
                        mapper.writeValueAsString(emailList));
                adminEmailList.addAll(emailList);
            } catch (IOException e) {
                log.warn("Failed to patch /SuperAdminEmails", e);
            }
        }

        if (isToBeOverwritten(plsLatticeAdmins)) {
            List<String> emailList = new ArrayList<>();
            for (String email : Arrays.asList(plsLatticeAdmins.split(LIST_DELIMITER))) {
                if (!StringUtils.isEmpty(email))
                    emailList.add(email);
            }
            emailList.removeAll(adminEmailList);
            try {
                serviceService.patchDefaultConfig(PLSComponent.componentName, "/LatticeAdminEmails",
                        mapper.writeValueAsString(emailList));
            } catch (IOException e) {
                log.warn("Failed to patch /LatticeAdminEmails", e);
            }
        }
    }

    private boolean isToBeOverwritten(String value) {
        return value != null && !"DEFAULT".equals(value);
    }

}
