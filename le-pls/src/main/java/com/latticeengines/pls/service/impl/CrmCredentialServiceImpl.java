package com.latticeengines.pls.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.service.CrmCredentialService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component("crmService")
public class CrmCredentialServiceImpl implements CrmCredentialService {

    private static final Log log = LogFactory.getLog(CrmCredentialServiceImpl.class);

    @Autowired
    TenantConfigServiceImpl tenantConfigService;

    @Autowired
    DataLoaderService dataLoaderService;

    @Autowired
    CrmCredentialZKService crmCredentialZKService;

    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microServiceUrl;

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
        String orgId = getSfdcOrgId(crmCredential, isProduction);
        newCrmCredential.setOrgId(orgId);
        crmCredential.setOrgId(orgId);

        FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(tenantId);
        if (!useEaiToValidate(flags)) {
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
            dataLoaderService.verifyCredentials(crmType, crmCredential, isProduction, dlUrl);
        } else {
            validateCredentialUsingEai(tenantId, crmType, crmCredential, isProduction);
        }
        writeToZooKeeper(crmType, tenantId, isProduction, crmCredential, true);

        return newCrmCredential;
    }

    private boolean useEaiToValidate(FeatureFlagValueMap flags) {
        return flags.containsKey(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName()));
    }

    @VisibleForTesting
    void validateCredentialUsingEai(String tenantId, String crmType, CrmCredential crmCredential, Boolean isProduction) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        RestTemplate restTemplate = new RestTemplate();
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace.toString());
        uriVariables.put("sourceType", CrmConstants.CRM_SFDC);

        String url = "https://login.salesforce.com";
        if (!isProduction) {
            url = url.replace("://login", "://test");
        }

        crmCredential.setUrl(url);
        String password = crmCredential.getPassword();

        try {
            crmCredential.setPassword(CipherUtils.encrypt(password));
            ResponseDocument<?> response = SimpleBooleanResponse.emptyFailedResponse(Collections.<String> emptyList());
            response = restTemplate.postForObject(microServiceUrl
                    + "/eai/validatecredential/customerspaces/{customerSpace}/sourcetypes/{sourceType}", crmCredential,
                    SimpleBooleanResponse.class, uriVariables);
            if (!response.isSuccess()) {
                throw new RuntimeException("Validation Failed!");
            }
            crmCredential.setPassword(password);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18030, e);
        }
    }

    private CrmCredential updateMarketoConfig(String crmType, String tenantId, CrmCredential crmCredential) {

        CrmCredential newCrmCredential = new CrmCredential(crmCredential);
        String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
        dataLoaderService.verifyCredentials(crmType, crmCredential, true, dlUrl);
        writeToZooKeeper(crmType, tenantId, true, crmCredential, false);

        return newCrmCredential;

    }

    private CrmCredential updateEloquaConfig(String crmType, String tenantId, CrmCredential crmCredential) {
        CrmCredential newCrmCredential = new CrmCredential(crmCredential);
        String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
        dataLoaderService.verifyCredentials(crmType, crmCredential, true, dlUrl);
        writeToZooKeeper(crmType, tenantId, true, crmCredential, false);

        return newCrmCredential;

    }

    private void writeToZooKeeper(String crmType, String tenantId, Boolean isProduction, CrmCredential crmCredential,
            boolean isWriteCustomerSpace) {
        crmCredentialZKService.writeToZooKeeper(crmType, tenantId, isProduction, crmCredential, isWriteCustomerSpace);
    }

    private String getSfdcOrgId(CrmCredential crmCredential, boolean isProduction) {
        String url = "https://login.salesforce.com/services/oauth2/token";
        if (!isProduction) {
            url = url.replace("://login", "://test");
        }
        MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
        parameters.add("grant_type", "password");
        parameters.add("client_id", "3MVG9fMtCkV6eLhdjB5FspKNuLjXBEL0Qe1dDCYZTL.z0kfLUbkW4Tj0XV_x395LX7F_1XOjoaQ==");
        parameters.add("client_secret", "129111989871209262");
        parameters.add("username", crmCredential.getUserName());
        String password = crmCredential.getPassword();
        if (!StringUtils.isEmpty(crmCredential.getSecurityToken())) {
            password += crmCredential.getSecurityToken();
        }
        parameters.add("password", password);
        parameters.add("format", "json");
        try {
            RestTemplate restTemplate = new RestTemplate();
            String result = restTemplate.postForObject(url, parameters, String.class);
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(result);

            String id = (String) jsonObject.get("id");

            String[] tokens = id.split("/");
            return tokens[tokens.length - 2];
        } catch (Exception ex) {
            log.warn(
                    "Failed to get " + (isProduction ? "production" : "sandbox") + " sfdcOrgId for "
                            + crmCredential.toString(), ex);
            throw new LedpException(LedpCode.LEDP_18030, ex);
        }
    }

    @Override
    public CrmCredential getCredential(String crmType, String tenantId, Boolean isProduction) {
        return crmCredentialZKService.getCredential(crmType, tenantId, isProduction);
    }

    @Override
    public void removeCredentials(String crmType, String tenantId, Boolean isProduction) {
        crmCredentialZKService.removeCredentials(crmType, tenantId, isProduction);
    }

    @VisibleForTesting
    void setMicroServiceUrl(String microServiceUrl) {
        this.microServiceUrl = microServiceUrl;
    }

}
