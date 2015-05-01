package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CreateVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.GetVisiDBDLRequest;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class VisiDBDLInstaller extends LatticeComponentInstaller {

    private static final Log log = LogFactory.getLog(VisiDBDLInstaller.class);

    private String dlUrl;

    private static final int SUCCESS = 3;

    public VisiDBDLInstaller() {
        super(VisiDBDLComponent.componentName);
    }

    protected void setDLUrl(String dlUrl){
        this.dlUrl = dlUrl;
    }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        String tenant = getData(configDir, "Tenant");
        String tenantAlias = getData(configDir, "TenantAlias");
        String dmDeployment = getData(configDir, "DMDeployment");
        String contractExternalID = getData(configDir, "ContractExternalID");

        String createNewVisiDB = getChild(configDir, "VisiDB", "CreateNewVisiDB").getDocument().getData();
        String visiDBName = getChild(configDir, "VisiDB", "VisiDBName").getDocument().getData();
        String visiDBServerName = "ServerName=" + getChild(configDir, "VisiDB", "ServerName").getDocument().getData();
        String ownerEmail = getChild(configDir, "DL", "OwnerEmail").getDocument().getData();

        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("MagicAuthentication", "Security through obscurity!"));
        headers.add(new BasicNameValuePair("Content-Type", "application/json"));
        headers.add(new BasicNameValuePair("Accept", "application/json"));

        GetVisiDBDLRequest getRequest = new GetVisiDBDLRequest(tenant, tenantAlias, dmDeployment, contractExternalID,
                visiDBName, visiDBServerName);
        Map<String, Object> response = getTenantInfo(getRequest, headers);
        if ((Integer)response.get("Status") != SUCCESS) {
            CreateVisiDBDLRequest postRequest = new CreateVisiDBDLRequest(tenant, tenantAlias, ownerEmail,
                    dmDeployment, contractExternalID, Boolean.parseBoolean(createNewVisiDB), visiDBName,
                    visiDBServerName);
            response = createTenant(postRequest, headers);
            log.info("response is: " + response);
            if((Integer)response.get("Status")!= SUCCESS){
                throw new LedpException(LedpCode.LEDP_18032);
            }
        }else if(response.get("ErrorMessage") == null){
            log.info("Tenant is already in VisiDB/Dataloader");
        }else{
            throw new LedpException(LedpCode.LEDP_18032, new String[]{(String)response.get("ErrorMessage")});
        }
    }

    public Map<String, Object> getTenantInfo(GetVisiDBDLRequest getRequest, List<BasicNameValuePair> headers){
        String jsonString = JsonUtils.serialize(getRequest);
        String response = "";
        try {
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/GetDLTenantSettings", false,
                    headers, jsonString);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        return convertToMap(response);
    }
    
    public Map<String, Object> createTenant(CreateVisiDBDLRequest postRequest, List<BasicNameValuePair> headers){
        String jsonString = JsonUtils.serialize(postRequest);
        String response = "";
        try {
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/createDLTenant",
                    false, headers, jsonString);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        return convertToMap(response);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> convertToMap(String response){
        Map<String, Object> map = new HashMap<>();
        try {
            map = new ObjectMapper().readValue(response, Map.class);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        return map;
    }
}
