package com.latticeengines.admin.tenant.batonadapter.template.dl;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;

import com.latticeengines.admin.dynamicopts.impl.TemplateProvider;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.DLRestResult;
import com.latticeengines.domain.exposed.admin.InstallTemplateRequest;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;


public class DLTemplateInstaller extends LatticeComponentInstaller {

    private static final Log log = LogFactory.getLog(DLTemplateInstaller.class);

    private TenantService tenantService;

    private TemplateProvider templateProvider;

    private static final int SUCCESS = 3;

    public DLTemplateInstaller() { super(DLTemplateComponent.componentName); }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }

    public void setTemplateProvider(TemplateProvider templateProvider) {
        this.templateProvider = templateProvider;
    }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        String dmDeployment = space.getTenantId();
        String contractExternalID = space.getContractId();

        TenantDocument tenantDoc = tenantService.getTenant(contractExternalID, dmDeployment);
        String tenant = dmDeployment;
        String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
        CRMTopology topology = tenantDoc.getSpaceConfig().getTopology();

        SerializableDocumentDirectory vdbdlConfig;
        try {
            vdbdlConfig = tenantService.getTenantServiceConfig(
                    contractExternalID, dmDeployment, VisiDBDLComponent.componentName);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18038, "Cannot find the configuration of VisiDBDL component.", e);
        }

        String version = vdbdlConfig.getNodeAtPath("/TemplateVersion").getData();
        File dataloaderTemplate = new File(templateProvider.getTemplate(version, topology) + ".config");

        try {
            String str = IOUtils.toString(new InputStreamReader(new FileInputStream(dataloaderTemplate)));
            InstallTemplateRequest request = new InstallTemplateRequest(tenant, str);
            DLRestResult response = installDataloaderTemplate(request, getHeaders(), dlUrl);
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
    }

    public DLRestResult installDataloaderTemplate(InstallTemplateRequest request, List<BasicNameValuePair> headers,
            String dlUrl) throws IOException {
        String jsonStr = JsonUtils.serialize(request);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl
                + "/DLRestService/InstallConfigFile_Sync", false, headers, jsonStr);
        return JsonUtils.deserialize(response, DLRestResult.class);
    }
}
