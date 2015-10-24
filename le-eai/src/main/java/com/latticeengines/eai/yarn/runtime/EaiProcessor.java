package com.latticeengines.eai.yarn.runtime;

import java.util.List;
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

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.appmaster.service.EaiAppmasterService;
import com.latticeengines.eai.routes.marketo.MarketoRouteConfig;
import com.latticeengines.eai.routes.salesforce.SalesforceRouteConfig;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

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

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {

        CamelContext camelContext = constructCamelContext(importConfig);
        camelContext.start();
        log.info("Routes are:" + camelContext.getRoutes());
        importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
        log.info("Starting extract and import.");
        try {
            List<Table> tableMetadata = dataExtractionService.extractAndImport(importConfig, importContext);

            while (camelContext.getInflightRepository().size() > 0) {
                Thread.sleep(5000L);
            }
            log.info("Finished extract and import.");

            eaiMetadataService.setLastModifiedTimeStamp(tableMetadata, importContext);
            eaiMetadataService.registerTables(tableMetadata, importContext);
        } catch (Exception e) {
            Thread.sleep(20000);
            dataExtractionService.cleanUpTargetPathData(importContext);
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
        return camelContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
