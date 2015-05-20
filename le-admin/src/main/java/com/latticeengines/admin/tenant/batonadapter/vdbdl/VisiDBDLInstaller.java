package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CreateVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.DLRestResult;
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

    private static final int MASTER = 1;

    private static final int STANDALONE = 0;

    public VisiDBDLInstaller() {
        super(VisiDBDLComponent.componentName);
    }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        String dmDeployment = space.getTenantId();
        String contractExternalID = space.getContractId();

        TenantDocument tenantDoc = tenantService.getTenant(contractExternalID, dmDeployment);
        String tenant = dmDeployment;
        String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();

        String tenantAlias = getData(configDir, "TenantAlias");
        String createNewVisiDB = getChild(configDir, "VisiDB", "CreateNewVisiDB").getDocument().getData();
        String caseSensitive = getChild(configDir, "VisiDB", "CaseSensitive").getDocument().getData();
        String visiDBName = getChild(configDir, "VisiDB", "VisiDBName").getDocument().getData();
        String visiDBFileDirectory = getChild(configDir, "VisiDB", "VisiDBFileDirectory").getDocument().getData();
        String cacheLimit = getChild(configDir, "VisiDB", "CacheLimit").getDocument().getData();
        String diskspaceLimit = getChild(configDir, "VisiDB", "DiskspaceLimit").getDocument().getData();
        String permanentStoreOption = getChild(configDir, "VisiDB", "PermanentStoreOption").getDocument().getData();
        String permanentStorePath = getChild(configDir, "VisiDB", "PermanentStorePath").getDocument().getData();
        String visiDBServerName = getChild(configDir, "VisiDB", "ServerName").getDocument().getData();
        String ownerEmail = getChild(configDir, "DL", "OwnerEmail").getDocument().getData();
        String dataStorePath = getChild(configDir, "DL", "DataStorePath").getDocument().getData();

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
            DLRestResult response = getTenantInfo(getRequest, getHeaders(), dlUrl);
            int status = response.getStatus();
            String errorMessage = response.getErrorMessage();

            if (status != SUCCESS && !StringUtils.isEmpty(errorMessage) && errorMessage.contains("does not exist")) {
                createPermstoreFolder(permanentStorePath, visiDBServerName);
                createDataStoreFolder(dataStorePath, dmDeployment);
                CreateVisiDBDLRequest.Builder builder = new CreateVisiDBDLRequest.Builder(tenant, dmDeployment,
                        contractExternalID);
                builder.tenantAlias(tenantAlias).ownerEmail(ownerEmail).visiDBName(visiDBName)
                        .visiDBLocation("ServerName=" + visiDBServerName).visiDBFileDirectory(visiDBFileDirectory)
                        .createNewVisiDB(Boolean.parseBoolean(createNewVisiDB))
                        .caseSensitive(Boolean.parseBoolean(caseSensitive)).cacheLimit(Integer.parseInt(cacheLimit))
                        .diskspaceLimit(Integer.parseInt(diskspaceLimit)).permanentStoreOption(permStoreOpt)
                        .permanentStorePath(permanentStorePath);
                CreateVisiDBDLRequest postRequest = builder.build();
                response = createTenant(postRequest, getHeaders(), dlUrl);
                status = response.getStatus();
                if (status != SUCCESS) {
                    throw new LedpException(LedpCode.LEDP_18032, new String[] { response.getErrorMessage() });
                }
                log.info("Tenant " + tenant + " has been successfully created in VisiDB/Dataloader");
            } else if (StringUtils.isEmpty(errorMessage) && status == SUCCESS) {
                log.info("Tenant " + tenant + " has already been installed in VisiDB/Dataloader");
            } else {
                throw new LedpException(LedpCode.LEDP_18032, new String[] { errorMessage });
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18032, e);
        }
    }

    private void createDataStoreFolder(String dataStorePath, String dmDeployment) {
        new File(dataStorePath + "/" + dmDeployment + "/backup").mkdirs();
        new File(dataStorePath + "/" + dmDeployment + "/launch").mkdirs();
        new File(dataStorePath + "/" + dmDeployment + "/status").mkdirs();
    }

    private void createPermstoreFolder(String permanentStorePath, String visiDBServerName) {
        new File(permanentStorePath + "/" + visiDBServerName.toUpperCase()).mkdir();
    }

    public DLRestResult getTenantInfo(GetVisiDBDLRequest getRequest, List<BasicNameValuePair> headers, String dlUrl) throws IOException {
        String jsonString = JsonUtils.serialize(getRequest);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/GetDLTenantSettings",
                true, getHeaders(), jsonString);
        return JsonUtils.deserialize(response, DLRestResult.class);
    }

    public DLRestResult createTenant(CreateVisiDBDLRequest postRequest, List<BasicNameValuePair> headers, String dlUrl)
            throws IOException {
        String jsonString = JsonUtils.serialize(postRequest);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/CreateDLTenant",
                false, headers, jsonString);
        return JsonUtils.deserialize(response, DLRestResult.class);
    }

}
