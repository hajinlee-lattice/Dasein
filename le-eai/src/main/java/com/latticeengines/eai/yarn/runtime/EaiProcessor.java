package com.latticeengines.eai.yarn.runtime;

import org.apache.camel.CamelContext;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.apache.camel.spring.SpringCamelContext;
import org.apache.hadoop.conf.Configuration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.appmaster.service.EaiAppmasterService;
import com.latticeengines.eai.routes.marketo.MarketoRouteConfig;
import com.latticeengines.eai.routes.salesforce.SalesforceRouteConfig;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

@Component
public class EaiProcessor extends SingleContainerYarnProcessor<ImportConfiguration> implements
        ItemProcessor<ImportConfiguration, String>, ApplicationContextAware {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private ImportContext importContext;

    private ApplicationContext applicationContext;

    @Autowired
    private SalesforceComponent salesforce;

    @Autowired
    private MarketoRouteConfig marketoRouteConfig;

    @Autowired
    private SalesforceRouteConfig salesforceRouteConfig;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private EaiAppmasterService eaiAppmasterService;
    
    @Override
    public String process(ImportConfiguration importConfig) throws Exception {
        try {
            CamelContext camelContext = constructCamelContext(importConfig);
            camelContext.start();
            importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
            importContext.setProperty(ImportProperty.TARGETPATH, importConfig.getTargetPath());
            log.info("Starting extract and import.");
            dataExtractionService.extractAndImport(importConfig, importContext);
            
            while (camelContext.getInflightRepository().size() > 0) {
                Thread.sleep(10000L);
            }
            log.info("Finished extract and import.");
        } catch (Exception e) {
            Thread.sleep(20000);
            eaiAppmasterService.handleException(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        return null;
    }

    private CamelContext constructCamelContext(ImportConfiguration importConfig) throws Exception {
        String tenantId = importConfig.getCustomer();
        CrmCredential crmCredential = crmCredentialZKService.getCredential(CrmConstants.CRM_SFDC, tenantId, true);

        SalesforceLoginConfig loginConfig = salesforce.getLoginConfig();
        loginConfig.setUserName(crmCredential.getUserName());
        loginConfig.setPassword(crmCredential.getPassword());

        CamelContext camelContext = new SpringCamelContext(applicationContext);
        camelContext.addRoutes(salesforceRouteConfig);
        camelContext.addRoutes(marketoRouteConfig);
        log.info("Routes are:" + camelContext.getRoutes());
        Thread.sleep(5000);
        return camelContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
