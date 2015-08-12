package com.latticeengines.eai.service.impl.marketo;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.ProducerTemplate;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.routes.ImportProperty;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;
import com.latticeengines.eai.service.ImportService;

@ContextConfiguration(locations = { "classpath:test-eai-context.xml", "classpath:eai-yarn-context.xml" })
public class MarketoImportServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private ImportService marketoImportService;
    
    @Autowired
    private ProducerTemplate producerTemplate;
    
    @Autowired
    private Configuration yarnConfiguration;
    
    private SourceImportConfiguration marketoImportConfig = new SourceImportConfiguration();
    private ImportContext ctx = new ImportContext();
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/Activity");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/ActivityType");
        ctx.setProperty(MarketoImportProperty.HOST, "976-KKC-431.mktorest.com");
        ctx.setProperty(MarketoImportProperty.CLIENTID, "c98abab9-c62d-4723-8fd4-90ad5b0056f3");
        ctx.setProperty(MarketoImportProperty.CLIENTSECRET, "PlPMqv2ek7oUyZ7VinSCT254utMR0JL5");
        ctx.setProperty(ImportProperty.PRODUCERTEMPLATE, producerTemplate);
        ctx.setProperty(ImportProperty.HADOOPCONFIG, yarnConfiguration);
        ctx.setProperty(ImportProperty.TARGETPATH, "/tmp");
        
        List<Table> tables = new ArrayList<>();
        Table activityType = createMarketoActivityType();
        Table lead = createMarketoLead();
        Table activity = createMarketoActivity();
        tables.add(activityType);
        tables.add(lead);
        tables.add(activity);
        
        marketoImportConfig.setTables(tables);
        marketoImportConfig.setFilter(activity.getName(), "activityDate > '2014-10-01' AND activityTypeId IN (1, 12)");
    }
    
    @Test(groups = "functional", enabled = true)
    public void importMetadata() {
        List<Table> tables = marketoImportService.importMetadata(marketoImportConfig, ctx);
        
        for (Table table : tables) {
            for (Attribute attribute : table.getAttributes()) {
                assertNotNull(attribute.getPhysicalDataType());
            }
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "importMetadata" }, enabled = true)
    public void importDataAndWriteToHdfs() throws Exception {
        marketoImportService.importDataAndWriteToHdfs(marketoImportConfig, ctx);
        Thread.sleep(10000L);
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, "/tmp/Activity"));
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, "/tmp/ActivityType"));
        List<String> filesForActivity = HdfsUtils.getFilesForDir(yarnConfiguration, "/tmp/Activity", new HdfsFilenameFilter() {

            @Override
            public boolean accept(String file) {
                return file.endsWith(".parquet");
            }
            
        });
        assertEquals(filesForActivity.size(), 1);
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, "/tmp/ActivityType"));
        List<String> filesForActivityType = HdfsUtils.getFilesForDir(yarnConfiguration, "/tmp/ActivityType", new HdfsFilenameFilter() {

            @Override
            public boolean accept(String file) {
                return file.endsWith(".parquet");
            }
            
        });
        assertEquals(filesForActivityType.size(), 1);
    }

}
