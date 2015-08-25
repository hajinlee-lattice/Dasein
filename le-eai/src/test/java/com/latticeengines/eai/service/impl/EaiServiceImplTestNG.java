package com.latticeengines.eai.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;

public class EaiServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private EaiService eaiService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private Configuration yarnConfiguration;

    protected static final Log log = LogFactory.getLog(EaiServiceImplTestNG.class);

    private String customer = "Eai-" + System.currentTimeMillis();

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/dataFromFile");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/Activity");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/ActivityType");
    }

    @Test(groups = { "functional", "functional.production" }, enabled = true)
    public void extractAndImport() throws Exception {
        SourceImportConfiguration marketoImportConfig = new SourceImportConfiguration();
        Map<String, String> props = new HashMap<>();

        props.put(MarketoImportProperty.HOST, "976-KKC-431.mktorest.com");
        props.put(MarketoImportProperty.CLIENTID, "c98abab9-c62d-4723-8fd4-90ad5b0056f3");
        props.put(MarketoImportProperty.CLIENTSECRET, "PlPMqv2ek7oUyZ7VinSCT254utMR0JL5");
        
        List<Table> tables = new ArrayList<>();
        Table activityType = createMarketoActivityType();
        Table lead = createMarketoLead();
        Table activity = createMarketoActivity();
        tables.add(activityType);
        tables.add(lead);
        tables.add(activity);
        
        marketoImportConfig.setSourceType(SourceType.MARKETO);
        marketoImportConfig.setTables(tables);
        marketoImportConfig.setFilter(activity.getName(), "activityDate > '2014-10-01' AND activityTypeId IN (1, 12)");
        marketoImportConfig.setProperties(props);
        String targetPath = "/tmp";
        ImportConfiguration importConfig = new ImportConfiguration();
        importConfig.setCustomer(customer);
        importConfig.addSourceConfiguration(marketoImportConfig);
        importConfig.setTargetPath(targetPath);
        ApplicationId appId = eaiService.extractAndImport(importConfig);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, "/tmp/Activity"));
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, "/tmp/ActivityType"));
        List<String> filesForActivity = HdfsUtils.getFilesForDir(yarnConfiguration, "/tmp/Activity", new HdfsFilenameFilter() {

            @Override
            public boolean accept(String file) {
                return file.endsWith(".avro");
            }

        });
        assertEquals(filesForActivity.size(), 1);
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, "/tmp/ActivityType"));
        List<String> filesForActivityType = HdfsUtils.getFilesForDir(yarnConfiguration, "/tmp/ActivityType", new HdfsFilenameFilter() {

            @Override
            public boolean accept(String file) {
                return file.endsWith(".avro");
            }

        });
        assertEquals(filesForActivityType.size(), 1);
    }
    
    @Test(groups = { "functional" }, enabled = true)
    public void extractAndImportForFile() throws Exception {
        URL dataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/exposed/service/impl");
        URL metadataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/exposed/service/impl/ConcurSampleMetadata.xml");
        SourceImportConfiguration fileImportConfig = new SourceImportConfiguration();
        Table file = createFile(dataUrl);
        fileImportConfig.setSourceType(SourceType.FILE);
        fileImportConfig.setTables(Arrays.<Table>asList(new Table[] { file }));
        Map<String, String> props = new HashMap<>();
        props.put(ImportProperty.DATAFILEDIR, dataUrl.getPath());
        props.put(ImportProperty.METADATAFILE, metadataUrl.getPath());
        fileImportConfig.setProperties(props);
        String targetPath = "/tmp/dataFromFile";
        ImportConfiguration importConfig = new ImportConfiguration();
        importConfig.setCustomer(customer);
        importConfig.addSourceConfiguration(fileImportConfig);
        importConfig.setTargetPath(targetPath);
        ApplicationId appId = eaiService.extractAndImport(importConfig);
        assertNotNull(appId);
    }
    
    private Table createFile(URL inputUrl) {
        String url = String.format("jdbc:relique:csv:%s", inputUrl.getPath());
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver).dbType("GenericJDBC");
        DbCreds creds = new DbCreds(builder);

        DataSchema schema = metadataService.createDataSchema(creds, "ConcurSample");

        Table file = new Table();
        file.setName("ConcurSample");

        for (Field field : schema.getFields()) {
            Attribute attr = new Attribute();
            attr.setName(field.getName());
            file.addAttribute(attr);
        }
        
       return file;
    }
}