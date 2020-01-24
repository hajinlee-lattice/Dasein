package com.latticeengines.eai.service.impl.marketo;

import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.inject.Inject;

import org.apache.camel.CamelContext;
import org.apache.camel.spring.SpringCamelContext;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.functionalframework.MarketoExtractAndImportUtil;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;
import com.latticeengines.eai.routes.marketo.MarketoRouteConfig;
import com.latticeengines.eai.service.ImportService;
public class MarketoImportServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Inject
    private ImportService marketoImportService;

    @Inject
    private Configuration yarnConfiguration;

    private SourceImportConfiguration marketoImportConfig = new SourceImportConfiguration();

    private ImportContext importContext;

    @Inject
    private MarketoRouteConfig marketoRouteConfig;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/Activity");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/ActivityType");

        CamelContext camelContext = new SpringCamelContext(applicationContext);
        camelContext.addRoutes(marketoRouteConfig);
        camelContext.start();

        importContext = new ImportContext(yarnConfiguration);
        importContext.setProperty(MarketoImportProperty.HOST, "976-KKC-431.mktorest.com");
        importContext.setProperty(MarketoImportProperty.CLIENTID, "868c37ad-905c-4562-be86-c6b1f39293f4");
        importContext.setProperty(MarketoImportProperty.CLIENTSECRET, "vBt3ZnFAU4eCyrtzOzRZfvkRQPfdDrUi");
        importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
        importContext.setProperty(ImportProperty.TARGETPATH, "/tmp");
        importContext.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());

        List<Table> tables = new ArrayList<>();
        Table activityType = MarketoExtractAndImportUtil.createMarketoActivityType();
        Table lead = MarketoExtractAndImportUtil.createMarketoLead();
        Table activity = MarketoExtractAndImportUtil.createMarketoActivity();
        
        System.out.println(activityType);
        System.out.println(lead);
        System.out.println(activity);
        
        tables.add(activityType);
        tables.add(lead);
        tables.add(activity);

        marketoImportConfig.setTables(tables);
        marketoImportConfig.setFilter(activity.getName(), "activityDate > '2014-10-01' AND activityTypeId IN (1, 12)");
    }

    @Test(groups = "functional", enabled = true)
    public void importMetadata() {
        List<Table> tables = marketoImportService.importMetadata(marketoImportConfig, importContext, null);

        for (Table table : tables) {
            System.out.println(table);
            for (Attribute attribute : table.getAttributes()) {
                assertNotNull(attribute.getPhysicalDataType());
            }
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "importMetadata" }, enabled = true)
    public void importDataAndWriteToHdfs() throws Exception {
        marketoImportService.importDataAndWriteToHdfs(marketoImportConfig, importContext, null);
        Thread.sleep(10000L);
        checkDataExists("/tmp", Arrays.<String> asList(new String[] { "Activity", "ActivityType" }), 1);
    }
}
