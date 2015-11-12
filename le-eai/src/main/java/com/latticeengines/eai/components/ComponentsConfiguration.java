package com.latticeengines.eai.components;

import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.eclipse.jetty.client.HttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ComponentsConfiguration {

    @Bean(name = "salesforce")
    public SalesforceComponent constructSalesforceComponent(@Value("${eai.salesforce.production.loginurl}") String loginUrl,
            @Value("${eai.salesforce.clientid}") String clientId,
            @Value("${eai.salesforce.clientsecret}") String clientSecret) {
        SalesforceComponent salesForce = new SalesforceComponent();
        SalesforceLoginConfig loginConfig = new SalesforceLoginConfig();
        SalesforceEndpointConfig config = new SalesforceEndpointConfig();
        HttpClient httpClient = new HttpClient();

        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.setConnectTimeout(60 * 1000);
        httpClient.setTimeout(60 * 60 * 1000);
        config.setHttpClient(httpClient);
        
        loginConfig.setClientId(clientId);
        loginConfig.setClientSecret(clientSecret);
        loginConfig.setLoginUrl(loginUrl);
        salesForce.setLoginConfig(loginConfig);
        salesForce.setConfig(config);
        return salesForce;
    }
}
