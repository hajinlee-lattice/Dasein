package com.latticeengines.eai.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.camel.CamelContext;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.apache.camel.spring.SpringCamelContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.routes.marketo.MarketoRouteConfig;
import com.latticeengines.eai.routes.salesforce.SalesforceRouteConfig;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-eai-context.xml" })
public class EaiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected static final Log log = LogFactory.getLog(EaiFunctionalTestNGBase.class);

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private YarnClient defaultYarnClient;

    @Autowired
    private MetadataService metadataService;

    protected DataPlatformFunctionalTestNGBase platformTestBase;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    @Autowired
    private SalesforceComponent salesforce;

    @Autowired
    private MarketoRouteConfig marketoRouteConfig;

    @Autowired
    private SalesforceRouteConfig salesforceRouteConfig;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
        platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

    protected Table createFile(URL inputUrl, String fileName) {
        String url = String.format("jdbc:relique:csv:%s", inputUrl.getPath());
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver).dbType("GenericJDBC");
        DbCreds creds = new DbCreds(builder);

        DataSchema schema = metadataService.createDataSchema(creds, fileName);

        Table file = new Table();
        file.setName(fileName);

        for (Field field : schema.getFields()) {
            Attribute attr = new Attribute();
            attr.setName(field.getName());
            file.addAttribute(attr);
        }

        return file;
    }

    protected void verifyAllDataNotNullWithNumRows(Configuration config, String targetDir, int expectedNumRows)
            throws Exception {
        List<String> avroFiles = HdfsUtils.getFilesByGlob(config, String.format("%s/*.avro", targetDir));

        int numRows = 0;
        for (String avroFile : avroFiles) {
            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(config, new org.apache.hadoop.fs.Path(
                    avroFile))) {
                while (reader.hasNext()) {
                    GenericRecord record = reader.next();
                    Schema schema = record.getSchema();
                    for (org.apache.avro.Schema.Field field : schema.getFields()) {
                        assertNotNull(record.get(field.name()));
                    }
                    numRows++;
                }
            }
        }
        assertEquals(numRows, expectedNumRows);
    }

    protected List<String> getFilesFromHdfs(String targetPath, String table) throws Exception {
        return HdfsUtils.getFilesForDirRecursive(yarnConfiguration, targetPath + "/" + table, new HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                return file.getPath().getName().endsWith(".avro");
            }

        });
    }

    protected void checkDataExists(String targetPath, List<String> tables) throws Exception {
        for (String table : tables) {
            assertTrue(HdfsUtils.fileExists(yarnConfiguration, targetPath + "/" + table));
            List<String> filesForTable = getFilesFromHdfs(targetPath, table);
            assertEquals(filesForTable.size(), 1);
        }
    }

    protected CamelContext constructCamelContext(ImportConfiguration importConfig) throws Exception {
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
}
