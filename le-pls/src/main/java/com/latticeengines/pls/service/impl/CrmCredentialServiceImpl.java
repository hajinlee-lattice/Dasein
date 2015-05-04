package com.latticeengines.pls.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HttpWithRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.service.CrmConstants;
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
    public CrmCredential verifyCredential(String crmType, String tenantId, Boolean isProduction,
            CrmCredential crmCredential) {
        switch (crmType) {
        case CrmConstants.CRM_SFDC:
            return updateSfdcConfig(crmType, tenantId, isProduction, crmCredential);
        case CrmConstants.CRM_MARKETO:
            return updateMarketoConfig(crmType, tenantId, crmCredential);
        case CrmConstants.CRM_ELOQUA:
            return updateEloquaConfig(crmType, tenantId, crmCredential);
        }

        return new CrmCredential();
    }

    private CrmCredential updateSfdcConfig(String crmType, String tenantId, Boolean isProduction,
            CrmCredential crmCredential) {

        CrmCredential newCrmCredential = new CrmCredential(crmCredential);
        String orgId = getSfdcOrgId(crmCredential);
        newCrmCredential.setOrgId(orgId);
        crmCredential.setOrgId(orgId);
        verifySfdcFromDataLoader(crmType, crmCredential);
        writeToZooKeeper(crmType, tenantId, isProduction, crmCredential, true);

        return newCrmCredential;
    }

    private CrmCredential updateMarketoConfig(String crmType, String tenantId, CrmCredential crmCredential) {

        CrmCredential newCrmCredential = new CrmCredential(crmCredential);
        verifyMarketoFromDataLoader(crmType, crmCredential);
        writeToZooKeeper(crmType, tenantId, true, crmCredential, false);

        return newCrmCredential;

    }

    private CrmCredential updateEloquaConfig(String crmType, String tenantId, CrmCredential crmCredential) {
        CrmCredential newCrmCredential = new CrmCredential(crmCredential);
        verifyEloquaFromDataLoader(crmType, crmCredential);
        writeToZooKeeper(crmType, tenantId, true, crmCredential, false);

        return newCrmCredential;

    }

    private void verifyEloquaFromDataLoader(String crmType, CrmCredential crmCredential) {

        String url = dataLoaderUrl + "/ValidateExternalAPICredentials";
        Map<String, String> parameters = new HashMap<>();
        parameters.put("company", crmCredential.getCompany());
        if (StringUtils.isEmpty(crmCredential.getUrl())) {
            crmCredential.setUrl(eloquaLoginUrl);
        }
        setCommonParameters(crmType, crmCredential, parameters);
        excuteHttpRequest(url, parameters);

    }

    private void verifyMarketoFromDataLoader(String crmType, CrmCredential crmCredential) {
        String url = dataLoaderUrl + "/ValidateExternalAPICredentials";

        Map<String, String> parameters = new HashMap<>();
        if (StringUtils.isEmpty(crmCredential.getUrl())) {
            crmCredential.setUrl(marketoLoginUrl);
        }
        setCommonParameters(crmType, crmCredential, parameters);
        excuteHttpRequest(url, parameters);
    }

    private void verifySfdcFromDataLoader(String crmType, CrmCredential crmCredential) {
        String url = dataLoaderUrl + "/ValidateExternalAPICredentials";

        Map<String, String> parameters = new HashMap<>();
        parameters.put("token", crmCredential.getSecurityToken());
        if (StringUtils.isEmpty(crmCredential.getUrl())) {
            crmCredential.setUrl(sfdcLoginUrl);
        }
        setCommonParameters(crmType, crmCredential, parameters);
        excuteHttpRequest(url, parameters);
    }

    private void setCommonParameters(String crmType, CrmCredential crmCredential, Map<String, String> parameters) {
        parameters.put("type", crmType);
        parameters.put("user", crmCredential.getUserName());
        parameters.put("password", crmCredential.getPassword());
        parameters.put("url", crmCredential.getUrl());
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

    private void writeToZooKeeper(String crmType, String tenantId, Boolean isProduction, CrmCredential crmCredential,
            boolean isWriteCustomerSpace) {
        try {
            if (isWriteCustomerSpace) {
                writeAsCustomerSpace(tenantId, isProduction, crmCredential);
            }

            crmCredential.setPassword(CipherUtils.encrypt(crmCredential.getPassword()));
            writeAsCredential(crmType, tenantId, isProduction, crmCredential);

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18030, ex);
        }
    }

    private void writeAsCredential(String crmType, String tenantId, Boolean isProduction, CrmCredential crmCredential)
            throws Exception {

        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), customerSpace.getContractId(),
                customerSpace.getTenantId(), customerSpace.getSpaceId());
        docPath = addExtraPath(crmType, docPath, isProduction);
        Document doc = new Document(JsonUtils.serialize(crmCredential));
        camille.upsert(docPath, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
    }

    private void writeAsCustomerSpace(String tenantId, Boolean isProduction, CrmCredential crmCredential)
            throws Exception {
        CustomerSpaceInfo spaceInfo = null;
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        try {
            spaceInfo = SpaceLifecycleManager.getInfo(customerSpace.getContractId(), customerSpace.getTenantId(),
                    customerSpace.getSpaceId());
        } catch (Exception ex) {
            log.warn("Space does not exist!");
        }
        if (spaceInfo == null) {
            spaceInfo = new CustomerSpaceInfo(new CustomerSpaceProperties(), "");
        } else if (spaceInfo.properties == null) {
            spaceInfo.properties = new CustomerSpaceProperties();
        }

        if (Boolean.FALSE.equals(isProduction)) {
            spaceInfo.properties.sandboxSfdcOrgId = crmCredential.getOrgId();
        } else {
            spaceInfo.properties.sfdcOrgId = crmCredential.getOrgId();
        }
        SpaceLifecycleManager.create(customerSpace.getContractId(), customerSpace.getTenantId(),
                customerSpace.getSpaceId(), spaceInfo);
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
    public CrmCredential getCredential(String crmType, String tenantId, Boolean isProduction) {

        try {

            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

            Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                    customerSpace.getContractId(), customerSpace.getTenantId(), customerSpace.getSpaceId());
            docPath = addExtraPath(crmType, docPath, isProduction);

            Camille camille = CamilleEnvironment.getCamille();
            Document doc = camille.get(docPath);
            CrmCredential crmCredential = JsonUtils.deserialize(doc.getData(), CrmCredential.class);
            crmCredential.setPassword(CipherUtils.decrypt(crmCredential.getPassword()));

            return crmCredential;

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18031, ex);
        }
    }

    private Path addExtraPath(String crmType, Path docPath, Boolean isProduction) {
        docPath = docPath.append(crmType);
        if (crmType.equalsIgnoreCase(CrmConstants.CRM_SFDC)) {
            docPath = docPath.append(isProduction ? "Production" : "Sandbox");
        }
        return docPath;
    }
}
