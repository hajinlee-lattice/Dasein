package com.latticeengines.eai.yarn.runtime;

import java.util.List;

import org.apache.camel.CamelContext;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.apache.camel.spring.SpringCamelContext;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.client.HttpClient;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.source.SourceCredentialType;
import com.latticeengines.eai.config.HttpClientConfig;
import com.latticeengines.eai.routes.marketo.MarketoRouteConfig;
import com.latticeengines.eai.routes.salesforce.SalesforceRouteConfig;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.EaiZKService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

public class EaiProcessor extends SingleContainerYarnProcessor<ImportConfiguration> implements
        ItemProcessor<ImportConfiguration, String>, ApplicationContextAware {

    private static final Log log = LogFactory.getLog(EaiProcessor.class);

    private ApplicationContext applicationContext;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private ImportContext importContext;

    @Autowired
    private SalesforceComponent salesforce;

    @Autowired
    private MarketoRouteConfig marketoRouteConfig;

    @Autowired
    private SalesforceRouteConfig salesforceRouteConfig;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Autowired
    private EaiZKService eaiZKService;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {
        CamelContext camelContext = constructCamelContext(importConfig);
        camelContext.start();

        int i = 1;
        setProgress(0.05f * i);
        log.info("Routes are:" + camelContext.getRoutes());
        importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
        importContext.setProperty(ImportProperty.METADATAURL, importConfig.getProperty(ImportProperty.METADATAURL));
        log.info("Starting extract and import.");

        try {
            List<Table> tableMetadata = dataExtractionService.extractAndImport(importConfig, importContext);

            while (camelContext.getInflightRepository().size() > 0) {
                setProgress(0.05f * (i + 2));
                Thread.sleep(5000L);
            }
            log.info("Finished extract and import.");

            eaiMetadataService.updateTableSchema(tableMetadata, importContext);
            eaiMetadataService.registerTables(tableMetadata, importContext);
            setProgress(0.95f);
        } catch (Exception e) {
            Thread.sleep(20000);
            dataExtractionService.cleanUpTargetPathData(importContext);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        return null;
    }

    private CamelContext constructCamelContext(ImportConfiguration importConfig) throws Exception {
        String customerSpace = importConfig.getCustomerSpace().toString();
        SourceCredentialType sourceCredentialType = importConfig.getSourceConfigurations().get(0)
                .getSourceCredentialType();
        CrmCredential crmCredential = crmCredentialZKService.getCredential(CrmConstants.CRM_SFDC, customerSpace,
                sourceCredentialType.isProduction());

        SalesforceLoginConfig loginConfig = salesforce.getLoginConfig();

        if (salesforce.getConfig() != null && salesforce.getConfig().getHttpClient() != null) {
            log.info("Http connnection timeout = " + salesforce.getConfig().getHttpClient().getConnectTimeout());
            log.info("Http response timeout = " + salesforce.getConfig().getHttpClient().getTimeout());
        } else {
            log.info("No salesforce endpoint configured.");
        }

        loginConfig.setUserName(crmCredential.getUserName());
        String password = crmCredential.getPassword();
        if (!StringUtils.isEmpty(crmCredential.getSecurityToken())) {
            password += crmCredential.getSecurityToken();
        }
        loginConfig.setPassword(password);
        loginConfig.setLoginUrl(crmCredential.getUrl());
        HttpClientConfig httpClientConfig = eaiZKService.getHttpClientConfig(customerSpace);
        HttpClient httpClient = salesforce.getConfig().getHttpClient();
        httpClient.setConnectTimeout(httpClientConfig.getConnectTimeout());
        httpClient.setTimeout(httpClientConfig.getImportTimeout());

        CamelContext camelContext = new SpringCamelContext(applicationContext);
        camelContext.addRoutes(salesforceRouteConfig);
        camelContext.addRoutes(marketoRouteConfig);
        return camelContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
