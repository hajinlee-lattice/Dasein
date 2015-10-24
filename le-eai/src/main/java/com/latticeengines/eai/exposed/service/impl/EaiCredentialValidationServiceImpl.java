package com.latticeengines.eai.exposed.service.impl;

import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.exposed.service.EaiCredentialValidationService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

@Component("eaiCredentialValidationService")
public class EaiCredentialValidationServiceImpl implements EaiCredentialValidationService {

    private Log log = LogFactory.getLog(EaiCredentialValidationServiceImpl.class);

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Value("${eai.salesforce.loginurl}")
    private String loginUrl;

    @Value("${eai.salesforce.clientid}")
    private String clientId;

    @Value("${eai.salesforce.clientsecret}")
    private String clientSecret;

    @Override
    public void validateCredential(String customerSpace, String crmType) {
        if (crmType.equals(CrmConstants.CRM_SFDC)) {
            validateCrmCredential(customerSpace);
        }
    }

    @Override
    public void validateCrmCredential(String customerSpace) {
        CrmCredential crmCredential = crmCredentialZKService.getCredential(CrmConstants.CRM_SFDC, customerSpace, true);
        validateCrmCredential(customerSpace, crmCredential.getUserName(), crmCredential.getPassword());
    }

    @VisibleForTesting
    void validateCrmCredential(String customerSpace, String username, String password) {
        SalesforceComponent salesforce = new SalesforceComponent();
        SalesforceLoginConfig loginConfig = new SalesforceLoginConfig();
        loginConfig.setClientId(clientId);
        loginConfig.setClientSecret(clientSecret);
        loginConfig.setLoginUrl(loginUrl);
        salesforce.setLoginConfig(loginConfig);
        loginConfig.setUserName(username);
        loginConfig.setPassword(password);

        try {
            salesforce.start();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_17004, new String[] { customerSpace });
        } finally {
            salesforce.setConfig(new SalesforceEndpointConfig());
            try {
                salesforce.stop();
            } catch (Exception e) {
                log.error("Cannot close connection for salesforce component");
            }
        }
    }
}
