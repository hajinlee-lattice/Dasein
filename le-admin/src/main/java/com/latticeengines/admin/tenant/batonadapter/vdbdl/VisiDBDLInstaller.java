package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CreateVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.GetVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class VisiDBDLInstaller extends LatticeComponentInstaller {

    private static final Log log = LogFactory.getLog(VisiDBDLInstaller.class);

    private TenantService tenantService;

    private static final int SUCCESS = 3;

    private static final int EMPTY_STATUS = 0;

    private static final int MASTER = 1;

    private static final int STANDALONE = 0;

    public VisiDBDLInstaller() {
        super(VisiDBDLComponent.componentName);
    }

    private String dlUrl;

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

        String tenantAlias = getData(configDir, "TenantAlias");
        String createNewVisiDB = getChild(configDir, "VisiDB", "CreateNewVisiDB").getDocument().getData();
        String caseSensitive = getChild(configDir, "VisiDB", "CaseSensitive").getDocument().getData();
        String visiDBName = getChild(configDir, "VisiDB", "VisiDBName").getDocument().getData();
        String visiDBFileDirectory = getChild(configDir, "VisiDB", "VisiDBFileDirectory").getDocument().getData();
        String cacheLimit = getChild(configDir, "VisiDB", "CacheLimit").getDocument().getData();
        String diskspaceLimit = getChild(configDir, "VisiDB", "DiskspaceLimit").getDocument().getData();
        String permanentStoreOption = getChild(configDir, "VisiDB", "PermanentStoreOption").getDocument().getData();
        String permanentStorePath = getChild(configDir, "VisiDB", "PermanentStorePath").getDocument().getData();
        String visiDBServerName = "ServerName=" + getChild(configDir, "VisiDB", "ServerName").getDocument().getData();
        String ownerEmail = getChild(configDir, "DL", "OwnerEmail").getDocument().getData();

        if (StringUtils.isEmpty(tenantAlias)) {
            tenantAlias = tenant;
        }
        int permStoreOpt = MASTER;
        if (permanentStoreOption.toLowerCase().equals("master")) {
            permStoreOpt = MASTER;
        } else if (permanentStoreOption.toLowerCase().equals("standalone")) {
            permStoreOpt = STANDALONE;
        }

        GetVisiDBDLRequest getRequest = new GetVisiDBDLRequest(tenant);
        try {
            JsonNode response = getTenantInfo(getRequest, getHeaders());
            int status = response.get("Status").asInt();
            String errorMessage = response.get("ErrorMessage").asText();

            if (status == EMPTY_STATUS) {
                throw new LedpException(LedpCode.LEDP_18032, new String[] { "Status is null" });
            }
            if (!StringUtils.isEmpty(errorMessage) && errorMessage.contains("does not exist")) {
                CreateVisiDBDLRequest.Builder builder = new CreateVisiDBDLRequest.Builder(tenant, dmDeployment,
                        contractExternalID);
                builder.tenantAlias(tenantAlias).ownerEmail(ownerEmail).visiDBName(visiDBName)
                        .visiDBLocation(visiDBServerName).visiDBFileDirectory(visiDBFileDirectory)
                        .createNewVisiDB(Boolean.parseBoolean(createNewVisiDB))
                        .caseSensitive(Boolean.parseBoolean(caseSensitive)).cacheLimit(Integer.parseInt(cacheLimit))
                        .diskspaceLimit(Integer.parseInt(diskspaceLimit)).permanentStoreOption(permStoreOpt)
                        .permanentStorePath(permanentStorePath);
                CreateVisiDBDLRequest postRequest = builder.build();
                response = createTenant(postRequest, getHeaders());
                status = response.get("Status").asInt();
                if (status != SUCCESS) {
                    throw new LedpException(LedpCode.LEDP_18032, new String[] { response.get("ErrorMessage").asText() });
                }
                log.info("Tenant " + tenant + " has been successfully created in VisiDB/Dataloader");
            } else if (StringUtils.isEmpty(errorMessage) && status == SUCCESS) {
                log.info("Tenant " + tenant + " is already in VisiDB/Dataloader");
            } else {
                throw new LedpException(LedpCode.LEDP_18032, new String[] { errorMessage });
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18032, e);
        }
    }

    public JsonNode getTenantInfo(GetVisiDBDLRequest getRequest, List<BasicNameValuePair> headers) throws IOException {
        String jsonString = JsonUtils.serialize(getRequest);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/GetDLTenantSettings",
                true, getHeaders(), jsonString);
        return convertToJsonNode(response);
    }

    public JsonNode createTenant(CreateVisiDBDLRequest postRequest, List<BasicNameValuePair> headers)
            throws IOException {
        String jsonString = JsonUtils.serialize(postRequest);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/CreateDLTenant",
                false, headers, jsonString);
        return convertToJsonNode(response);
    }

}
