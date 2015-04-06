package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.functionalframework.ModelingServiceExecutor;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;

public class EndToEndDeploymentTestNG extends PlsFunctionalTestNGBase {

    @Value("${pls.modelingservice.rest.endpoint.hostport}")
    private String modelingServiceHostPort;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${pls.modelingservice.testdsdb}")
    private String dataSourceDb;

    @Value("${pls.modelingservice.testdsdbhost}")
    private String dataSourceHost;

    @Value("${pls.modelingservice.testdsdbuser}")
    private String dataSourceUser;

    @Value("${pls.modelingservice.testdsdbpasswd.encrypted}")
    private String dataSourcePassword;

    @Value("${pls.modelingservice.testdsdbport}")
    private int dataSourcePort;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    private static String tenant;
    private static Tenant tenantToAttach;

    @BeforeClass(groups = "deployment", enabled = true)
    public void setup() throws Exception {
        setupUsers();

        Ticket ticket = globalAuthenticationService.authenticateUser(adminUsername, DigestUtils.sha256Hex(adminPassword));
        assertTrue(ticket.getTenants().size() >= 2);
        assertNotNull(ticket);
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();
        tenantToAttach = ticket.getTenants().get(1);
        tenant = tenantToAttach.getId();
        setupDb(tenant1, tenant2, false);

        globalAuthenticationService.discard(ticket);
    }
    
    private ModelingServiceExecutor buildModel(String tenant, String modelName, String metadata, String table) throws Exception {
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(
                String.format("com/latticeengines/pls/controller/metadata-%s.avsc", metadata));
        String metadataContents = new String(IOUtils.toByteArray(modelSummaryFileAsStream));

        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        bldr.modelingServiceHostPort(modelingServiceHostPort) //
        .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
        .yarnConfiguration(yarnConfiguration) //
        .customer(tenant) //
        .dataSourceDb(dataSourceDb) //
        .dataSourceHost(dataSourceHost) //
        .dataSourceUser(dataSourceUser) //
        .dataSourcePassword(dataSourcePassword) //
        .dataSourcePort(dataSourcePort) //
        .dataSourceDbType("SQLServer") //
        .table(table) //
        .metadataTable("EventMetadata") //
        .keyColumn("ModelingID") //
        .profileExcludeList("LeadID", //
                "Email", //
                "ModelingID", //
                "P1_Event", //
                "CreationDate", //
                "Company", //
                "LastName", //
                "FirstName") //
        .targets("Event: P1_Event", //
                 "Readouts: LeadID | Email | CreationDate", //
                 "Company: Company", //
                 "LastName: LastName", //
                 "FirstName: FirstName", //
                 "SpamIndicator: SpamIndicator") //
        .metadataContents(metadataContents) //
        .modelName(modelName);

        ModelingServiceExecutor executor = new ModelingServiceExecutor(bldr);
        
        return executor;
    }

    
    @Test(groups = "deployment", enabled = true, dataProvider = "modelMetadataProvider")
    public void runPipeline(String tenant, String modelName, String metadataSuffix, String tableName) throws Exception {
        ModelingServiceExecutor executor = buildModel(tenant, modelName, metadataSuffix, tableName);
        executor.init();
        executor.runPipeline();
        Thread.sleep(30000L);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "runPipeline" })
    public void checkModels() {
        UserDocument doc = loginAndAttachAdmin(tenantToAttach);
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
        List response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), 2);
        Map<String, String> map = (Map) response.get(0);
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"), ModelSummary.class);
        assertTrue(summary.getName().startsWith("PLSModel-Eloqua"));
        assertNotNull(summary.getDetails());
    }
    
    @DataProvider(name = "modelMetadataProvider")
    public static Object[][] getModelMetadataProvider() {
        return new Object[][] { //
                { tenant, "PLSModel-Eloqua1", "eloqua1", "Q_PLS_Modeling_Tenant1" }, //
                { tenant, "PLSModel-Eloqua2", "eloqua2", "Q_PLS_Modeling_Tenant2" }
        };
    }
}
