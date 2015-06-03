package com.latticeengines.admin.tenant.batonadapter.template.visidb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
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
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.InstallTemplateRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.remote.exposed.service.Headers;

public class VisiDBTemplateInstaller extends LatticeComponentInstaller {

    private static final Log log = LogFactory.getLog(VisiDBTemplateInstaller.class);

    private TenantService tenantService;

    private TemplateProvider templateProvider;

    private static final int SUCCESS = 3;

    public VisiDBTemplateInstaller() {
        super(VisiDBTemplateComponent.componentName);
    }

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
        File visiDBTemplate = new File(templateProvider.getTemplate(version, topology) + ".specs");

        try {
            String str = IOUtils.toString(new BOMInputStream(new FileInputStream(visiDBTemplate)));
            InstallTemplateRequest request = new InstallTemplateRequest(tenant, str);
            InstallResult response = installVisiDBTemplate(request, Headers.getHeaders(), dlUrl);
            if (response != null && response.getStatus() == SUCCESS) {
                log.info("Template " + topology.name() + " has successfully been installed!");
            } else {
                throw new LedpException(LedpCode.LEDP_18036, new String[] { response.getErrorMessage() });
            }
        } catch (FileNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_18023, e);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18036, e);
        }
    }

    public InstallResult installVisiDBTemplate(InstallTemplateRequest request, List<BasicNameValuePair> headers,
            String dlUrl) throws IOException {
        String jsonStr = JsonUtils.serialize(request);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl
                + "/DLRestService/InstallVisiDBStructureFile_Sync", false, headers, jsonStr);
        return JsonUtils.deserialize(response, InstallResult.class);
    }
}
