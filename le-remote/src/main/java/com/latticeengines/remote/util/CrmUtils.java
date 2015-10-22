package com.latticeengines.remote.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.domain.exposed.pls.CrmCredential;

public class CrmUtils {

    public static final String CRM_SFDC = CRMTopology.SFDC.getName().toLowerCase();
    public static final String CRM_MARKETO = CRMTopology.MARKETO.getName().toLowerCase();
    public static final String CRM_ELOQUA = CRMTopology.ELOQUA.getName().toLowerCase();
    public static final String CRM_ELOQUA_BULK = "EloquaBulk";

    private static final Map<String, String> loginUrls = new HashMap<>();

    private CrmUtils() { }

    static {
        loginUrls.put(CRM_ELOQUA, "https://login.eloqua.com/id");
        loginUrls.put(CRM_ELOQUA_BULK, "https://login.eloqua.com/id");
        loginUrls.put(CRM_SFDC, "https://login.salesforce.com/services/Soap/u/33.0");
    }

    public static Map<String, String> verificationParameters(String crmType, CrmCredential crmCredential,
                                                             boolean isProduction) {
        crmCredential.setUrl(crmUrl(crmType, crmCredential, isProduction));

        Map<String, String> parameters = new HashMap<>();
        parameters.put("type", crmType);
        parameters.put("user", crmCredential.getUserName());
        parameters.put("password", crmCredential.getPassword());
        parameters.put("url", crmCredential.getUrl());
        if (crmType.equals(CRM_ELOQUA)) {
            parameters.put("company", crmCredential.getCompany());
        }
        if (crmType.equals(CRM_SFDC)) {
            parameters.put("token", crmCredential.getSecurityToken());
        }
        return parameters;
    }

    public static Map<String, Object> dataProviderParameters(String crmType, String tenantId, CrmConfig crmConfig) {
        CustomerSpace space = CustomerSpace.parse(tenantId);
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("dataProviderName", crmConfig.getDataProviderName());
        parameters.put("dataSourceType", crmType);
        parameters.put("tenantName", space.getTenantId());
        parameters.put("tryConnect", "false");

        if (crmType.equals(CRM_ELOQUA) || crmType.endsWith(CRM_ELOQUA_BULK)) {
            Map<String, String> values = new HashMap<>();
            values.put("URL", crmUrl(crmType, crmConfig.getCrmCredential(), true));
            values.put("Username", crmConfig.getCrmCredential().getUserName());
            values.put("Password", crmConfig.getCrmCredential().getPassword());
            values.put("Company", crmConfig.getCrmCredential().getCompany());
            values.put("EntityType", "Base");
            parameters.put("values", toDictFormat(values));
        }

        if (crmType.equals(CRM_MARKETO)) {
            Map<String, String> values = new HashMap<>();
            values.put("URL", crmUrl(crmType, crmConfig.getCrmCredential(), true));
            values.put("UserID", crmConfig.getCrmCredential().getUserName());
            values.put("EncryptionKey", crmConfig.getCrmCredential().getPassword());
            parameters.put("values", toDictFormat(values));
        }

        if (crmType.equals(CRM_SFDC)) {
            Map<String, String> values = new HashMap<>();
            values.put("URL", crmUrl(crmType, crmConfig.getCrmCredential(), true));
            values.put("User", crmConfig.getCrmCredential().getUserName());
            values.put("Password", crmConfig.getCrmCredential().getPassword());
            values.put("SecurityToken", crmConfig.getCrmCredential().getSecurityToken());
            String version = crmConfig.getVersion();
            if (!org.apache.commons.lang3.StringUtils.isBlank(version)) {
                values.put("Version", version);
            }
            parameters.put("values", toDictFormat(values));
        }

        return parameters;
    }

    private static String crmUrl(String crmType, CrmCredential crmCredential, boolean isProduction) {

        String url;

        if (crmType.equals(CRM_MARKETO)) {
            url = crmCredential.getUrl();
        } else {
            url = loginUrls.get(crmType);
            if (crmType.equals(CRM_SFDC) && !isProduction) {
                url = url.replace("://login", "://test");
            }
        }

        return url;
    }
    
    public static boolean checkVerificationStatus(String jsonResponse) throws Exception {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = ( JSONObject ) jsonParser.parse(jsonResponse);
        JSONArray jsonArray = ( JSONArray ) jsonObject.get("Value");
        JSONObject result = ( JSONObject ) jsonArray.get(0);
        return "Effective".equals(result.get("Key")) && "true".equals(result.get("Value"));
    }

    public static boolean checkUpdateDataProviderStatus(String status) throws Exception {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = ( JSONObject ) jsonParser.parse(status);
        Long statusCode = ( Long ) jsonObject.get("Status");
        if (statusCode != null && statusCode == 3L) {
            return true;
        }
        Boolean isSuccessful = ( Boolean ) jsonObject.get("Success");
        if (isSuccessful != null && isSuccessful) {
            return true;
        }
        String errorMsg = ( String ) jsonObject.get("ErrorMessage");
        throw new RuntimeException(errorMsg);
    }

    private static List<InstallResult.ValueResult> toDictFormat(Map<String, String> values) {
        List<InstallResult.ValueResult> valueResults = new ArrayList<>();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            InstallResult.ValueResult valueResult = new InstallResult.ValueResult();
            valueResult.setKey(entry.getKey());
            valueResult.setValue(entry.getValue());
            valueResults.add(valueResult);
        }

        return valueResults;
    }
}
