package com.latticeengines.eai.service.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import org.apache.avro.generic.GenericRecord;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.metadata.util.EaiMetadataUtil;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;

public class DataExtractionServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private DataExtractionServiceImpl dataExtractionService;

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;

    private String customer = "SFDC-Eai-Customer";

    @Autowired
    private ImportContext importContext;

    @Value("${eai.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.salesforce.password}")
    private String salesforcePasswd;

    @Value("${eai.salesforce.production.loginurl}")
    private String productionLoginUrl;

    private String targetPath;

    private EaiMetadataServiceImpl eaiMetadataService;

    private List<String> tableNameList = Arrays.<String> asList(new String[] { "Account", "Contact", "Lead",
            "Opportunity", "OpportunityContactRole" });

    private List<String> attributes = Arrays.<String> asList(new String[] { "LastModifiedDate" });

    private final Map<String, LastModifiedKey> map = ImmutableMap.of("Account", new LastModifiedKey(attributes,
            1442544230000L), "Contact", new LastModifiedKey(attributes, 1223400194000L), "Lead", new LastModifiedKey(
            attributes, 1237387254000L), "Opportunity", new LastModifiedKey(attributes, 1346770851000L),
            "OpportunityContactRole", new LastModifiedKey(attributes, 1341330034000L));

    @BeforeClass(groups = "functional")
    private void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer)
                .toString());
        crmCredentialZKService.removeCredentials(customer, customer, true);
        targetPath = dataExtractionService.createTargetPath(customer);
        initZK(customer);
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName(salesforceUserName);
        crmCredential.setPassword(salesforcePasswd);
        crmCredential.setUrl(productionLoginUrl);
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);

        eaiMetadataService = mock(EaiMetadataServiceImpl.class);
        when(eaiMetadataService.getLastModifiedKey(any(String.class), any(Table.class))).thenAnswer(
                new Answer<LastModifiedKey>() {

                    @Override
                    public LastModifiedKey answer(InvocationOnMock invocation) throws Throwable {

                        Object[] args = invocation.getArguments();
                        Table t = (Table) args[1];
                        return map.get(t.getName());
                    }

                });
        when(eaiMetadataService.getImportTables(any(String.class))).thenReturn(getSalesforceTables(tableNameList));
        dataExtractionService.setEaiMetadataService(eaiMetadataService);

    }

    @AfterClass(groups = "functional")
    private void cleanUp() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer)
                .toString());
        Camille camille = CamilleEnvironment.getCamille();
        camille.delete(PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer));
    }

    @Test(groups = "functional", enabled = true)
    public void extractAndImport() throws Exception {
        ImportConfiguration importConfig = createSalesforceImportConfig(customer);
        CamelContext camelContext = constructCamelContext(importConfig);
        camelContext.start();
        importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
        List<Table> tables = dataExtractionService.extractAndImport(importConfig, importContext);

        Thread.sleep(30000L);

        new EaiMetadataServiceImpl().updateTableSchema(tables, importContext);
        for (Table table : tables) {
            System.out.println(JsonUtils.serialize(table));
        }
        checkDataExists(targetPath, tableNameList, 1);
        System.out.println(importContext.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class));

        dataExtractionService.cleanUpTargetPathData(importContext);
        checkDataExists(targetPath, tableNameList, 0);
        checkExtractsDirectoryExists(customer, tableNameList);
    }

    @Test(groups = "functional", enabled = true)
    public void testAttributeWithSemanticType() throws Exception {
        ImportConfiguration importConfig = createSalesforceImportConfig(customer);
        CamelContext camelContext = constructCamelContext(importConfig);
        camelContext.start();

        Table table = new Table();
        table.setName("Account");
        Attribute attr = new Attribute();
        attr.setName("Id");
        attr.setSemanticType("NewId");
        attr.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        attr.setDataSource("");
        attr.setDataQuality("");
        attr.setDescription("");
        attr.setDisplayDiscretizationStrategy("");
        attr.setDisplayName("");
        attr.setCategory("");
        attr.setDataType("");
        attr.setFundamentalType("");
        attr.setPhysicalName(attr.getName());
        attr.setStatisticalType("");
        attr.setTags(Arrays.asList(new String[] { ModelingMetadata.INTERNAL_TAG }));
        LastModifiedKey lmk = EaiMetadataUtil.createLastModifiedKey();
        lmk.setLastModifiedTimestamp(1000000000000L);
        table.setLastModifiedKey(lmk);
        table.setAttributes(Arrays.<Attribute> asList(new Attribute[] { attr }));
        when(eaiMetadataService.getImportTables(any(String.class))).thenReturn(
                Arrays.<Table> asList(new Table[] { table }));

        importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
        List<Table> tables = dataExtractionService.extractAndImport(importConfig, importContext);
        new EaiMetadataServiceImpl().updateTableSchema(tables, importContext);
        assertFalse(tables.get(0).getNameAttributeMap().containsKey("Id"));
        assertTrue(tables.get(0).getNameAttributeMap().containsKey("NewId"));

        Thread.sleep(30000L);
        checkDataExists(targetPath, Arrays.<String> asList(new String[] { "Account" }), 1);
        System.out.println(importContext.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class));
        List<String> filesForTable = getFilesFromHdfs(targetPath, table.getName());
        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, new Path(filesForTable.get(0)));
        for (GenericRecord record : records) {
            String name = String.valueOf(record.get("NewId"));
            assertTrue(StringUtils.isNotEmpty(name));
        }

        dataExtractionService.cleanUpTargetPathData(importContext);
        checkDataExists(targetPath, Arrays.<String> asList(new String[] { "Account" }), 0);
        checkExtractsDirectoryExists(customer, Arrays.<String> asList(new String[] { "Account" }));
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "functional")
    public void interceptExceptionTest() throws Exception {
        ImportConfiguration importConfig = createSalesforceImportConfig(customer);
        CamelContext camelContext = constructCamelContext(importConfig);
        RouteDefinition route = camelContext.getRouteDefinitions().get(0);
        route.adviceWith(camelContext, new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                interceptSendToEndpoint("salesforce:closeJob").skipSendToOriginalEndpoint().process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        throw new Exception("Inject Exception to the exchange");
                    }
                });
            }
        });

        camelContext.start();
        importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
        try {
            dataExtractionService.extractAndImport(importConfig, importContext);
        } catch (Exception e) {
            assertTrue(e.getCause().toString().contains("Inject Exception to the exchange"));
            @SuppressWarnings("unchecked")
            Map<String, String> map = importContext.getProperty(ImportProperty.EXTRACT_PATH, HashMap.class);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                assertTrue(entry.getValue().endsWith(".avro"));
                assertTrue(HdfsUtils.fileExists(yarnConfiguration, entry.getValue()));
            }
        }
    }

    @Test(groups = "functional")
    public void setFilters() throws IOException {
        ImportConfiguration importConfig = createSalesforceImportConfig(customer);
        SourceImportConfiguration sourceImportConfig = importConfig.getSourceConfigurations().get(0);

        sourceImportConfig.setTables(getSalesforceTables(tableNameList));
        List<Table> tableMetadata = getSalesforceTables(tableNameList);
        for (Table table : tableMetadata) {
            sourceImportConfig.setFilter(table.getName(), null);
            assertNull(sourceImportConfig.getFilter(table.getName()));
        }

        dataExtractionService.setFilters(sourceImportConfig, customer);
        tableMetadata = sourceImportConfig.getTables();
        for (Table table : tableMetadata) {
            String filter = sourceImportConfig.getFilter(table.getName());
            System.out.println(filter);
            long timeStamp = map.get(table.getName()).getLastModifiedTimestamp();
            assertTrue(filter.contains(String.valueOf(new DateTime(timeStamp))));
        }
    }

}
