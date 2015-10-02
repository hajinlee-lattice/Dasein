package com.latticeengines.eai.service.impl.marketo;

import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.camel.CamelContext;
import org.apache.camel.spring.SpringCamelContext;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private ImportService marketoImportService;

    @Autowired
    private Configuration yarnConfiguration;

    private SourceImportConfiguration marketoImportConfig = new SourceImportConfiguration();

    @Autowired
    private ImportContext importContext;

    @Autowired
    private MarketoRouteConfig marketoRouteConfig;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/Activity");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/ActivityType");

        CamelContext camelContext = new SpringCamelContext(applicationContext);
        camelContext.addRoutes(marketoRouteConfig);
        camelContext.start();

        importContext.setProperty(MarketoImportProperty.HOST, "976-KKC-431.mktorest.com");
        importContext.setProperty(MarketoImportProperty.CLIENTID, "c98abab9-c62d-4723-8fd4-90ad5b0056f3");
        importContext.setProperty(MarketoImportProperty.CLIENTSECRET, "PlPMqv2ek7oUyZ7VinSCT254utMR0JL5");
        importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
        importContext.setProperty(ImportProperty.TARGETPATH, "/tmp");
        importContext.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());

        List<Table> tables = new ArrayList<>();
        Table activityType = MarketoExtractAndImportUtil.createMarketoActivityType();
        Table lead = MarketoExtractAndImportUtil.createMarketoLead();
        Table activity = MarketoExtractAndImportUtil.createMarketoActivity();
        tables.add(activityType);
        tables.add(lead);
        tables.add(activity);

        marketoImportConfig.setTables(tables);
        marketoImportConfig.setFilter(activity.getName(), "activityDate > '2014-10-01' AND activityTypeId IN (1, 12)");
    }

    @Test(groups = "functional", enabled = true)
    public void importMetadata() {
        List<Table> tables = marketoImportService.importMetadata(marketoImportConfig, importContext);

        for (Table table : tables) {
            for (Attribute attribute : table.getAttributes()) {
                assertNotNull(attribute.getPhysicalDataType());
            }
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "importMetadata" }, enabled = true)
    public void importDataAndWriteToHdfs() throws Exception {
        marketoImportService.importDataAndWriteToHdfs(marketoImportConfig, importContext);
        Thread.sleep(10000L);
        checkDataExists("/tmp", Arrays.<String>asList(new String[]{"Activity", "ActivityType"}), 1);
    }
}
