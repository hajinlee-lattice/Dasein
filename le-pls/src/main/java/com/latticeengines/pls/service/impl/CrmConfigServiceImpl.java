package com.latticeengines.pls.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HttpWithRetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.pls.service.CrmConfigService;
import com.latticeengines.pls.service.CrmConstants;

@Component("crmConfigService")
@Lazy(value = true)
public class CrmConfigServiceImpl implements CrmConfigService {

    private static final Log log = LogFactory.getLog(CrmConfigServiceImpl.class);

    @Value("${pls.dataloader.rest.api}")
    private String dataLoaderUrl;
    @Value("${pls.dataloader.sfdc.login.url}")
    private String sfdcLoginUrl;
    @Value("${pls.dataloader.marketo.login.url}")
    private String marketoLoginUrl;
    @Value("${pls.dataloader.eloqua.login.url}")
    private String eloquaLoginUrl;

    @Override
    public void config(String crmType, String tenantId, CrmConfig crmConfig) {
        switch (crmType) {
        case CrmConstants.CRM_SFDC:
            sfdcConfig(crmType, tenantId, crmConfig);
            break;
        case CrmConstants.CRM_MARKETO:
            marketoConfig(crmType, tenantId, crmConfig);
            break;
        case CrmConstants.CRM_ELOQUA:
            eloquaConfig(crmType, tenantId, crmConfig);
            break;
        }

    }

    private void eloquaConfig(String crmType, String tenantId, CrmConfig crmConfig) {

        String url = dataLoaderUrl + "/UpdateDataProvider";
        Map<String, Object> parameters = new HashMap<>();
        Map<String, String> values = new HashMap<>();
        values.put("URL", eloquaLoginUrl);
        values.put("Username", crmConfig.getCrmCredential().getUserName());
        values.put("Password", crmConfig.getCrmCredential().getPassword());
        values.put("Company", crmConfig.getCrmCredential().getCompany());
        values.put("EntityType", "Base");
        parameters.put("values", values);
        
        crmConfig.setDataProviderName("Eloqua_DataProvider");
        setCommonParameters(crmType, tenantId, crmConfig, parameters);
        excuteHttpRequest(url, parameters);
        
        crmConfig.setDataProviderName("Eloqua_Bulk_DataProvider");
        setCommonParameters(crmType, tenantId, crmConfig, parameters);
        excuteHttpRequest(url, parameters);
        
        

    }

    private void marketoConfig(String crmType, String tenantId, CrmConfig crmConfig) {
        String url = dataLoaderUrl + "/UpdateDataProvider";

        Map<String, Object> parameters = new HashMap<>();
        Map<String, String> values = new HashMap<>();
        values.put("URL", marketoLoginUrl);
        values.put("UserID", crmConfig.getCrmCredential().getUserName());
        values.put("Password", crmConfig.getCrmCredential().getPassword());
        values.put("EncryptionKey", crmConfig.getCrmCredential().getSecurityToken());
        parameters.put("values", values);
        crmConfig.setDataProviderName("Marketo_DataProvider");

        setCommonParameters(crmType, tenantId, crmConfig, parameters);
        excuteHttpRequest(url, parameters);
    }

    private void sfdcConfig(String crmType, String tenantId, CrmConfig crmConfig) {
        String url = dataLoaderUrl + "/UpdateDataProvider";

        Map<String, Object> parameters = new HashMap<>();
        Map<String, String> values = new HashMap<>();
        values.put("URL", sfdcLoginUrl);
        values.put("User", crmConfig.getCrmCredential().getUserName());
        values.put("Password", crmConfig.getCrmCredential().getPassword());
        values.put("SecurityToken", crmConfig.getCrmCredential().getSecurityToken());
        values.put("Version", crmConfig.getVersion());
        parameters.put("values", values);
        crmConfig.setDataProviderName("SFDC_DataProvider");
        
        setCommonParameters(crmType, tenantId, crmConfig, parameters);
        excuteHttpRequest(url, parameters);
    }

    private void setCommonParameters(String crmType, String tenantId, CrmConfig crmConfig,
            Map<String, Object> parameters) {

        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        parameters.put("dataProviderName", crmConfig.getDataProviderName());
        parameters.put("dataSourceType", crmType);
        parameters.put("tenantName", customerSpace.getTenantId());
        parameters.put("tryConnect", "false");
    }

    void excuteHttpRequest(String url, Map<String, Object> parameters) {
        Map<String, String> headers = new HashMap<>();
        headers.put("MagicAuthentication", "Security through obscurity!");
        headers.put("Charset", "UTF-8");
        try {
            String status = HttpWithRetryUtils.executePostRequest(url, parameters, headers);
            checkStatus(status);

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18035, ex, new String[] { ex.getMessage() } );
        }
    }

    boolean checkStatus(String status) throws Exception {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(status);
        Long statusCode = (Long) jsonObject.get("Status");
        if (statusCode != null && statusCode == 3L) {
            return true;
        }
        Boolean isSuccessful = (Boolean)jsonObject.get("Success");
        if (isSuccessful != null && isSuccessful == true) {
            return true;
        }
        String errorMsg = (String) jsonObject.get("ErrorMessage");
        throw new RuntimeException(errorMsg);
    }

}
