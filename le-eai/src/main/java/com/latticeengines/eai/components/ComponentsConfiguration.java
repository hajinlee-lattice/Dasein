package com.latticeengines.eai.components;

import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ComponentsConfiguration {

    @Bean(name = "salesforce")
    public SalesforceComponent constructSalesforceComponent(@Value("${eai.salesforce.loginurl}") String loginUrl,
            @Value("${eai.salesforce.clientid}") String clientId,
            @Value("${eai.salesforce.clientsecret}") String clientSecret) {
        SalesforceComponent salesForce = new SalesforceComponent();
        SalesforceLoginConfig loginConfig = new SalesforceLoginConfig();
        loginConfig.setClientId(clientId);
        loginConfig.setClientSecret(clientSecret);
        loginConfig.setLoginUrl(loginUrl);
        salesForce.setLoginConfig(loginConfig);
        return salesForce;
    }
}
