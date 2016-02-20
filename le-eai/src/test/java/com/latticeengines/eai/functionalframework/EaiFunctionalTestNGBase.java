package com.latticeengines.eai.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
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

    @Value("${eai.test.metadata.url}")
    protected String mockMetadataUrl;

    @Value("${eai.test.metadata.port}")
    protected int mockPort;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
        platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

    protected void waitForCamelMessagesToComplete(CamelContext camelContext) throws Exception {
        while (camelContext.getInflightRepository().size() > 0) {
            Thread.sleep(5000L);
        }
    }

    protected void cleanupCamilleAndHdfs(String customer) throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, //
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer).toString());
        Camille camille = CamilleEnvironment.getCamille();
        try {
            camille.delete(PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer));
        } catch (Exception e) {
            log.warn(e);
        }
    }

    protected void initZK(String customer) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customer), "Eai");
        try {
            camille.delete(docPath);
        } catch (Exception e) {
        }

        Path connectTimeoutDocPath = docPath.append("SalesforceEndpointConfig").append("HttpClient")
                .append("ConnectTimeout");
        camille.create(connectTimeoutDocPath, new Document("60000"), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        Path importTimeoutDocPath = docPath.append("SalesforceEndpointConfig").append("HttpClient")
                .append("ImportTimeout");
        camille.create(importTimeoutDocPath, new Document("3600000"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    protected Table createFile(File destDir, String fileName) {
        String url = String.format("jdbc:relique:csv:%s", destDir.getPath());
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver).dbType("GenericJDBC");
        DbCreds creds = new DbCreds(builder);

        // if Table contains duplicate column names or column with empty name, here it will throw exception
        DataSchema schema = metadataService.createDataSchema(creds, fileName);

        Table file = new Table();
        file.setName(fileName);
        file.setInterpretation(SchemaInterpretation.SalesforceLead.name());
        for (Field field : schema.getFields()) {
            Attribute attr = new Attribute();
            attr.setName(field.getName());
            file.addAttribute(attr);
        }

        return file;
    }

    protected void verifyAllDataNotNullWithNumRows(Configuration config, Table table, int expectedNumRows)
            throws Exception {
        List<Extract> extracts = table.getExtracts();
        int numRows = 0;
        for (Extract extract : extracts) {
            List<String> avroFiles = HdfsUtils.getFilesByGlob(config, extract.getPath());

            for (String avroFile : avroFiles) {
                try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(config,
                        new org.apache.hadoop.fs.Path(avroFile))) {
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

    protected void checkExtractsDirectoryExists(String targetPath, List<String> tables) throws Exception {
        for (String table : tables) {
            String path = targetPath + "/" + table + "/Extracts";
            assertTrue(HdfsUtils.fileExists(yarnConfiguration, path));
        }
    }

    protected CamelContext constructCamelContext(ImportConfiguration importConfig) throws Exception {
        String tenantId = importConfig.getCustomerSpace().toString();
        CrmCredential crmCredential = crmCredentialZKService.getCredential(CrmConstants.CRM_SFDC, tenantId, true);

        SalesforceLoginConfig loginConfig = salesforce.getLoginConfig();
        loginConfig.setUserName(crmCredential.getUserName());
        String password = crmCredential.getPassword();
        if (!StringUtils.isEmpty(crmCredential.getSecurityToken())) {
            password += crmCredential.getSecurityToken();
        }
        loginConfig.setPassword(password);

        CamelContext camelContext = new SpringCamelContext(applicationContext);
        camelContext.addRoutes(salesforceRouteConfig);
        camelContext.addRoutes(marketoRouteConfig);
        return camelContext;
    }

    protected ImportConfiguration createSalesforceImportConfig(String customer) {
        ImportConfiguration importConfig = new ImportConfiguration();
        SourceImportConfiguration salesforceConfig = new SourceImportConfiguration();
        salesforceConfig.setSourceType(SourceType.SALESFORCE);

        importConfig.setProperty(ImportProperty.METADATAURL, mockMetadataUrl);
        importConfig.addSourceConfiguration(salesforceConfig);
        importConfig.setCustomerSpace(CustomerSpace.parse(customer));
        return importConfig;
    }

    protected List<Table> getSalesforceTables(List<String> tableNameList) throws IOException {
        List<Table> tables = new ArrayList<>();

        for (String tableName : tableNameList) {
            URL url = ClassLoader.getSystemResource(String.format(
                    "com/latticeengines/eai/service/impl/salesforce/%s.json", tableName).toString());
            String str = FileUtils.readFileToString(new File(url.getFile()));
            Table table = JsonUtils.deserialize(str, Table.class);
            //DateTime date = new DateTime();
            //table.getLastModifiedKey().setLastModifiedTimestamp(date.minusMonths(6).getMillis());
            tables.add(table);
        }
        return tables;
    }

    protected Tenant createTenant(String customerSpace) {
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace);
        tenant.setName(customerSpace);
        tenant.setRegisteredTime(System.currentTimeMillis());
        return tenant;
    }

}
