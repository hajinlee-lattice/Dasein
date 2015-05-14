package com.latticeengines.admin.tenant.batonadapter.template.visidb;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.InstallVisiDBTemplateRequest;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class VisiDBTemplateInstaller extends LatticeComponentInstaller {

    private static final Log log = LogFactory.getLog(VisiDBTemplateInstaller.class);

    private TenantService tenantService;

    private String dlUrl;

    public VisiDBTemplateInstaller() {
        super(VisiDBTemplateComponent.componentName);
    }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        String dmDeployment = space.getTenantId();
        String contractExternalID = space.getContractId();

        TenantDocument tenantDoc = tenantService.getTenant(contractExternalID, dmDeployment);
        String tenant = tenantDoc.getTenantInfo().properties.displayName;
        dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
        String templatePath = tenantDoc.getSpaceConfig().getTemplatePath();
        CRMTopology topology = tenantDoc.getSpaceConfig().getTopology();
        if (topology.equals(CRMTopology.MARKETO)) {
            if (new File(templatePath, "Template_MKTO.specs").exists()) {
                System.out.println("exists!!!!!");
            }
        }

        try {
            File visiDBTemplate = new File(templatePath + "/Template_MKTO.specs");
            tenant = "haitao_test_tenant";
            String str = IOUtils.toString(new InputStreamReader(new FileInputStream(visiDBTemplate)));
            InstallVisiDBTemplateRequest request = new InstallVisiDBTemplateRequest(tenant, str);
            JsonNode response = installVisiDBTemplate(request, getHeaders());
            if (response.get("Status").asInt() == 3) {
                log.info("Succeeded!!!!!");
            }
        } catch (FileNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_18023, e);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18036, e);
        }
    }

    public JsonNode installVisiDBTemplate(InstallVisiDBTemplateRequest request, List<BasicNameValuePair> headers)
            throws IOException {
        String jsonStr = JsonUtils.serialize(request);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl
                + "/DLRestService/InstallVisiDBStructureFile_Sync", false, headers, jsonStr);
        return convertToJsonNode(response);
    }
}
