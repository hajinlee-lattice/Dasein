package com.latticeengines.pls.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooDefs;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HttpWithRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.service.CrmCredentialService;

@Component("crmService")
@Lazy(value = true)
public class CrmCredentialServiceImpl implements CrmCredentialService {

    private static final Log log = LogFactory.getLog(CrmCredentialServiceImpl.class);

    @Value("${pls.dataloader.rest.api}")
    private String dataLoaderUrl;
    @Value("${pls.dataloader.sfdc.login.url}")
    private String sfdcLoginUrl;
    @Value("${pls.dataloader.marketo.login.url}")
    private String marketoLoginUrl;
    @Value("${pls.dataloader.eloqua.login.url}")
    private String eloquaLoginUrl;

    @Override
    public CrmCredential verifyCredential(String crmType, String tenantId, String contractId, CrmCredential crmCredential) {
        switch (crmType) {
        case CRM_SFDC:
            return updateSfdcConfig(crmType, tenantId, contractId, crmCredential);
        case CRM_MARKETO:
            return updateMarketoConfig(crmType, tenantId, contractId, crmCredential);
        case CRM_ELOQUA:
            return updateEloquaConfig(crmType, tenantId, contractId, crmCredential);
        }

        return new CrmCredential();
    }

    private CrmCredential updateSfdcConfig(String crmType, String tenantId, String contractId, CrmCredential crmCredential) {

        CrmCredential newCrmCredential = new CrmCredential(crmCredential);
        String orgId = getSfdcOrgId(crmCredential);
        newCrmCredential.setOrgId(orgId);

        verifySfdcFromDataLoader(crmType, tenantId, contractId, newCrmCredential);
        writeToZooKeeper(crmType, tenantId, contractId, newCrmCredential);

        return newCrmCredential;
    }

    private CrmCredential updateMarketoConfig(String crmType, String tenantId, String contractId, CrmCredential crmCredential) {

        CrmCredential newCrmCredential = new CrmCredential(crmCredential);
        verifyMarketoFromDataLoader(crmType, tenantId, contractId, newCrmCredential);
        writeToZooKeeper(crmType, tenantId, contractId, newCrmCredential);
        return newCrmCredential;

    }

    private CrmCredential updateEloquaConfig(String crmType, String tenantId, String contractId, CrmCredential crmCredential) {
        CrmCredential newCrmCredential = new CrmCredential(crmCredential);
        verifyEloquaFromDataLoader(crmType, tenantId, contractId, newCrmCredential);
        writeToZooKeeper(crmType, tenantId, contractId, newCrmCredential);
        return newCrmCredential;

    }

    private void verifyEloquaFromDataLoader(String crmType, String tenantId, String contractId, CrmCredential newCrmCredential) {

        String url = dataLoaderUrl + "/ValidateExternalAPICredentials";
        Map<String, String> parameters = new HashMap<>();
        parameters.put("company", newCrmCredential.getCompany());
        newCrmCredential.setUrl(eloquaLoginUrl);
        setCommonParameters(crmType, newCrmCredential, parameters);
        excuteHttpRequest(url, parameters);

    }

    private void verifyMarketoFromDataLoader(String crmType, String tenantId, String contractId, CrmCredential newCrmCredential) {
        String url = dataLoaderUrl + "/ValidateExternalAPICredentials";

        Map<String, String> parameters = new HashMap<>();
        newCrmCredential.setUrl(marketoLoginUrl);
        setCommonParameters(crmType, newCrmCredential, parameters);
        excuteHttpRequest(url, parameters);
    }

    private void verifySfdcFromDataLoader(String crmType, String tenantId, String contractId, CrmCredential newCrmCredential) {
        String url = dataLoaderUrl + "/ValidateExternalAPICredentials";

        Map<String, String> parameters = new HashMap<>();
        parameters.put("token", newCrmCredential.getSecurityToken());
        newCrmCredential.setUrl(sfdcLoginUrl);
        setCommonParameters(crmType, newCrmCredential, parameters);
        excuteHttpRequest(url, parameters);
    }

    private void setCommonParameters(String crmType, CrmCredential newCrmCredential, Map<String, String> parameters) {
        parameters.put("type", crmType);
        parameters.put("user", newCrmCredential.getUserName());
        parameters.put("password", newCrmCredential.getPassword());
        parameters.put("url", newCrmCredential.getUrl());
    }

    private void excuteHttpRequest(String url, Map<String, String> parameters) {
        Map<String, String> headers = new HashMap<>();
        headers.put("MagicAuthentication", "Security through obscurity!");
        headers.put("charset", "utf-8");
        try {
            String status = HttpWithRetryUtils.executePostRequest(url, parameters, headers);
            if (!checkStatus(status)) {
                throw new LedpException(LedpCode.LEDP_18030);
            }
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18030, ex);
        }
    }

    private boolean checkStatus(String status) throws Exception {
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(status);
        JSONArray jsonArray = (JSONArray) jsonObject.get("Value");
        JSONObject result = (JSONObject) jsonArray.get(0);
        if ("Effective".equals(result.get("Key")) && "true".equals(result.get("Value"))) {
            return true;
        }
        return false;
    }

    private void writeToZooKeeper(String crmType, String tenantId, String contractId, CrmCredential newCrmCredential) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                    crmType);
            Document doc = new Document(JsonUtils.serialize(newCrmCredential));
            camille.upsert(docPath, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18030, ex);
        }
    }

    private String getSfdcOrgId(CrmCredential crmCredential) {
        String url = "https://login.salesforce.com/services/oauth2/token";
        MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("grant_type", "password");
        parameters.add("client_id", "3MVG9fMtCkV6eLhdjB5FspKNuLjXBEL0Qe1dDCYZTL.z0kfLUbkW4Tj0XV_x395LX7F_1XOjoaQ==");
        parameters.add("client_secret", "129111989871209262");
        parameters.add("username", crmCredential.getUserName());
        parameters.add("password", crmCredential.getPassword() + crmCredential.getSecurityToken());
        parameters.add("format", "json");
        RestTemplate restTemplate = new RestTemplate();
        String result = restTemplate.postForObject(url, parameters, String.class);
        try {
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(result);

            String id = (String) jsonObject.get("id");

            String[] tokens = id.split("/");
            return tokens[tokens.length - 2];
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18029, ex);
        }
    }

    @Override
    public CrmCredential getCredential(String crmType, String tenantId, String contractId) {

        try {
            Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                    crmType);
            Camille camille = CamilleEnvironment.getCamille();
            Document doc = camille.get(docPath);
            CrmCredential newCrmCredential = JsonUtils.deserialize(doc.getData(), CrmCredential.class);
            return newCrmCredential;
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18031, ex);
        }
    }

}
