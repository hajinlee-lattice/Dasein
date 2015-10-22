package com.latticeengines.eai.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;
import org.joda.time.DateTime;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.EaiMetadataService;
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

    private String targetPath;

    @Autowired
    private EaiMetadataService eaiMetadataService;

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
        BatonService baton = new BatonServiceImpl();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo();
        spaceInfo.properties = new CustomerSpaceProperties();
        spaceInfo.properties.displayName = "";
        spaceInfo.properties.description = "";
        spaceInfo.featureFlags = "";
        baton.createTenant(customer, customer, "defaultspaceId", spaceInfo);
        crmCredentialZKService.removeCredentials("sfdc", customer, true);
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010oIogZVEFGbL3n0qiAp6F66TC");
        crmCredentialZKService.writeToZooKeeper("sfdc", customer, true, crmCredential, true);

        setupSalesforceImportConfig(customer);

        EaiMetadataServiceImpl eaiMetadataService = mock(EaiMetadataServiceImpl.class);
        when(eaiMetadataService.getLastModifiedKey(any(String.class), any(Table.class))).thenAnswer(
                new Answer<LastModifiedKey>() {

                    @Override
                    public LastModifiedKey answer(InvocationOnMock invocation) throws Throwable {

                        Object[] args = invocation.getArguments();
                        Table t = (Table) args[1];
                        return map.get(t.getName());
                    }

                });
        dataExtractionService.setEaiMetadataService(eaiMetadataService);

    }

    @AfterClass(groups = "functional")
    private void cleanUp() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), customer)
                .toString());
        crmCredentialZKService.removeCredentials(customer, customer, true);
    }

    @Test(groups = "functional")
    public void extractAndImport() throws Exception {

        CamelContext camelContext = constructCamelContext(importConfig);
        camelContext.start();
        importContext.setProperty(ImportProperty.PRODUCERTEMPLATE, camelContext.createProducerTemplate());
        List<Table> tables = dataExtractionService.extractAndImport(importConfig, importContext);
        for (Table table : tables) {
            System.out.println(table);
        }

        Thread.sleep(30000L);
        checkDataExists(targetPath, tableNameList, 1);
        System.out.println(importContext.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class));

        dataExtractionService.cleanUpTargetPathData(importContext);
        checkDataExists(targetPath, tableNameList, 0);
        checkExtractsDirectoryExists(customer, tableNameList);
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "functional")
    public void interceptExceptionTest() throws Exception {
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
    public void setFilters() {
        SourceImportConfiguration sourceImportConfig = importConfig.getSourceConfigurations().get(0);

        List<Table> tableMetadata = sourceImportConfig.getTables();
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
