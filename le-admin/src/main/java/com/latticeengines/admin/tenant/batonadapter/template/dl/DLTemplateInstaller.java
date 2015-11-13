package com.latticeengines.admin.tenant.batonadapter.template.dl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.admin.dynamicopts.impl.TemplateProvider;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.admin.util.BOMUtils;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.InstallTemplateRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.remote.exposed.service.DataLoaderService;

public class DLTemplateInstaller extends LatticeComponentInstaller {

    private static final Log log = LogFactory.getLog(DLTemplateInstaller.class);

    private TenantService tenantService;

    private TemplateProvider templateProvider;

    private DataLoaderService dataLoaderService;

    private static final int SUCCESS = 3;

    public DLTemplateInstaller() {
        super(DLTemplateComponent.componentName);
    }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }

    public void setTemplateProvider(TemplateProvider templateProvider) {
        this.templateProvider = templateProvider;
    }

    public void setDataloaderService(DataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        String dmDeployment = space.getTenantId();
        String contractExternalID = space.getContractId();

        TenantDocument tenantDoc = tenantService.getTenant(contractExternalID, dmDeployment);
        String tenant = dmDeployment;
        String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
        CRMTopology topology = tenantDoc.getSpaceConfig().getTopology();

        SerializableDocumentDirectory vdbdlConfig;
        try {
            vdbdlConfig = tenantService.getTenantServiceConfig(contractExternalID, dmDeployment,
                    VisiDBDLComponent.componentName);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18038, "Cannot find the configuration of VisiDBDL component.", e);
        }

        String version = vdbdlConfig.getNodeAtPath("/TemplateVersion").getData();
        File dataloaderTemplate = new File(templateProvider.getTemplate(version, topology) + ".config");
        if (!dataloaderTemplate.exists()) {
            throw new LedpException(LedpCode.LEDP_18038, new IOException("Cannot find DL template at "
                    + dataloaderTemplate));
        }
        try {
            String str = BOMUtils.toString(new FileInputStream(dataloaderTemplate));
            InstallTemplateRequest request = new InstallTemplateRequest(tenant, str);
            InstallResult response = dataLoaderService.installDataLoaderConfigFile(request, dlUrl);
            if (response != null && response.getStatus() == SUCCESS) {
                log.info("Template " + topology.name() + " has successfully been installed!");
            } else {
                throw new LedpException(LedpCode.LEDP_18038, new String[] { response.getErrorMessage() });
            }
        } catch (FileNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_18023, e);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18038, e);
        }

        return configDir;
    }
}
