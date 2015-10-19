package com.latticeengines.eai.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.camel.CamelContext;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.component.salesforce.SalesforceLoginConfig;
import org.apache.camel.spring.SpringCamelContext;
import org.apache.camel.testng.AbstractCamelTestNGSpringContextTests;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.eai.routes.marketo.MarketoRouteConfig;
import com.latticeengines.eai.routes.salesforce.SalesforceRouteConfig;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;
import com.latticeengines.security.exposed.service.TenantService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-eai-context.xml" })
public class EaiFunctionalTestNGBase extends AbstractCamelTestNGSpringContextTests {

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
    protected SalesforceRouteConfig salesforceRouteConfig;

    @Autowired
    protected TenantService tenantService;

    protected ImportConfiguration importConfig;

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

    protected void checkDataExists(String targetPath, List<String> tables, int number) throws Exception {
        for (String table : tables) {
            List<String> filesForTable = getFilesFromHdfs(targetPath, table);
            assertEquals(filesForTable.size(), number);
        }
    }

    protected void checkExtractsDirectoryExists(String customer, List<String> tables) throws Exception {
        Path customerSpacePath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), customer, customer,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        for (String table : tables) {
            String path = (customerSpacePath + "/Data/Tables/" + table + "/Extracts").toString();
            assertTrue(HdfsUtils.fileExists(yarnConfiguration, path));
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

    protected void setupSalesforceImportConfig(String customer) {
        List<Table> tables = new ArrayList<>();
        Table lead = SalesforceExtractAndImportUtil.createLead();
        Table account = SalesforceExtractAndImportUtil.createAccount();
        Table opportunity = SalesforceExtractAndImportUtil.createOpportunity();
        Table contact = SalesforceExtractAndImportUtil.createContact();
        Table contactRole = SalesforceExtractAndImportUtil.createOpportunityContactRole();
        tables.add(lead);
        tables.add(account);
        tables.add(opportunity);
        tables.add(contact);
        tables.add(contactRole);

        importConfig = new ImportConfiguration();
        SourceImportConfiguration salesforceConfig = new SourceImportConfiguration();
        salesforceConfig.setSourceType(SourceType.SALESFORCE);
        salesforceConfig.setTables(tables);

        importConfig.addSourceConfiguration(salesforceConfig);
        importConfig.setCustomer(customer);
    }

    protected Tenant createTenant(String customerSpace) {
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace);
        tenant.setName(customerSpace);
        tenant.setRegisteredTime(System.currentTimeMillis());
        return tenant;
    }

}
