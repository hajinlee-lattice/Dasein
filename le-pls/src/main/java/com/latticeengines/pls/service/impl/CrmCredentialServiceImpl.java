package com.latticeengines.pls.service.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooDefs;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.CipherUtils;
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
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component("crmService")
public class CrmCredentialServiceImpl implements CrmCredentialService {

    private static final Log log = LogFactory.getLog(CrmCredentialServiceImpl.class);

    @Autowired
    TenantConfigServiceImpl tenantConfigService;

    @Autowired
    DataLoaderService dataLoaderService;

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
        String dlUrl = tenantConfigService.getDLRestServiceAddress(tenantId);
        dataLoaderService.verifyCredentials(crmType, crmCredential, isProduction, dlUrl);
        writeToZooKeeper(crmType, tenantId, isProduction, crmCredential, true);

        return newCrmCredential;
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
            log.warn("Failed to get " + (isProduction ? "production" : "sandbox") +
                    " sfdcOrgId for " + crmCredential.toString(), ex);
            throw new LedpException(LedpCode.LEDP_18030, ex);
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

    @Override
    public void removeCredentials(String crmType, String tenantId, Boolean isProduction) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

            Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                    customerSpace.getContractId(), customerSpace.getTenantId(), customerSpace.getSpaceId());
            docPath = addExtraPath(crmType, docPath, isProduction);

            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(docPath)) camille.delete(docPath);
            log.info(String.format("Removing %s.%s credentials from tenant %s.", crmType,
                    isProduction ? "Production" : "Sandbox", tenantId));
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
