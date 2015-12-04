package com.latticeengines.eai.service.impl;

import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.apache.commons.lang.StringUtils;
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
import com.latticeengines.domain.exposed.source.SourceCredentialType;
import com.latticeengines.eai.exposed.service.EaiCredentialValidationService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

@Component("eaiCredentialValidationService")
public class EaiCredentialValidationServiceImpl implements EaiCredentialValidationService {

    private Log log = LogFactory.getLog(EaiCredentialValidationServiceImpl.class);

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Value("${eai.salesforce.clientid}")
    private String clientId;

    @Value("${eai.salesforce.clientsecret}")
    private String clientSecret;

    @Override
    public void validateSourceCredential(String customerSpace, String sourceType,
            SourceCredentialType sourceCredentialType) {
        if (sourceType.equals(CrmConstants.CRM_SFDC)) {
            validateSourceCredential(customerSpace, sourceCredentialType);
        } else {
            throw new LedpException(LedpCode.LEDP_17008, new String[] { sourceType });
        }
    }

    @VisibleForTesting
    void validateSourceCredential(String customerSpace, SourceCredentialType sourceCredentialType) {
        CrmCredential crmCredential = crmCredentialZKService.getCredential(CrmConstants.CRM_SFDC, customerSpace,
                sourceCredentialType.isProduction());
        try {
            validateSourceCredential(customerSpace, crmCredential);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_17004, e, new String[] { customerSpace, CrmConstants.CRM_SFDC });
        }
    }

    @Override
    public void validateSourceCredential(String customerSpace, String sourceType, CrmCredential crmCredential) {
        if (sourceType.equals(CrmConstants.CRM_SFDC)) {
            try {
                validateSourceCredential(customerSpace, crmCredential);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_17004, e, new String[] { customerSpace, CrmConstants.CRM_SFDC });
            }
        } else {
            throw new LedpException(LedpCode.LEDP_17008, new String[] { sourceType });
        }
    }

    @VisibleForTesting
    void validateSourceCredential(String customerSpace, CrmCredential crmCredential) {
        SalesforceComponent salesforce = new SalesforceComponent();
        SalesforceLoginConfig loginConfig = new SalesforceLoginConfig();
        loginConfig.setClientId(clientId);
        loginConfig.setClientSecret(clientSecret);
        loginConfig.setLoginUrl(crmCredential.getUrl());
        salesforce.setLoginConfig(loginConfig);
        loginConfig.setUserName(crmCredential.getUserName());
        String password = crmCredential.getPassword();
        if (!StringUtils.isEmpty(crmCredential.getSecurityToken())) {
            password += crmCredential.getSecurityToken();
        }
        loginConfig.setPassword(password);
        log.info("username :" + crmCredential.getUserName());
        log.info("password :" + password);
        try {
            salesforce.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
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
