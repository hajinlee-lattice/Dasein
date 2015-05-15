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

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.DLRestResult;
import com.latticeengines.domain.exposed.admin.InstallVisiDBTemplateRequest;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class VisiDBTemplateInstaller extends LatticeComponentInstaller {

    private static final Log log = LogFactory.getLog(VisiDBTemplateInstaller.class);

    private TenantService tenantService;

    private static final String marketoTemplate = "Template_MKTO.specs";

    private static final String eloquaTemplate = "Template_ELQ.specs";

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
        String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
        String templatePath = tenantDoc.getSpaceConfig().getTemplatePath();
        CRMTopology topology = tenantDoc.getSpaceConfig().getTopology();
        File visiDBTemplate = getTemplateFile(topology, templatePath);

        try {
            String str = IOUtils.toString(new InputStreamReader(new FileInputStream(visiDBTemplate)));
            InstallVisiDBTemplateRequest request = new InstallVisiDBTemplateRequest(tenant, str);
            DLRestResult response = installVisiDBTemplate(request, getHeaders(), dlUrl);
            if (response != null && response.getStatus() == 3) {
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

    private File getTemplateFile(CRMTopology topology, String templatePath) {
        if (topology.equals(CRMTopology.MARKETO)) {
            return getFile(templatePath, marketoTemplate);
        } else if (topology.equals(CRMTopology.ELOQUA)) {
            return getFile(templatePath, eloquaTemplate);
        } else {
            throw new LedpException(LedpCode.LEDP_18037, new String[] { topology.name() });
        }
    }

    public File getFile(String templatePath, String templateFileName) {
        if (new File(templatePath, templateFileName).exists()) {
            return new File(templatePath + "/" + templateFileName);
        } else {
            throw new LedpException(LedpCode.LEDP_18023);
        }
    }

    public DLRestResult installVisiDBTemplate(InstallVisiDBTemplateRequest request, List<BasicNameValuePair> headers,
            String dlUrl) throws IOException {
        String jsonStr = JsonUtils.serialize(request);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl
                + "/DLRestService/InstallVisiDBStructureFile_Sync", false, headers, jsonStr);
        return JsonUtils.deserialize(response, DLRestResult.class);
    }
}
