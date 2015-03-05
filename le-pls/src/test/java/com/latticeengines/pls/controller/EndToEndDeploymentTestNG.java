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
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.functionalframework.ModelingServiceExecutor;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalAuthenticationServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalSessionManagementServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalUserManagementServiceImpl;
import com.latticeengines.pls.security.GrantedRight;

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
    private GlobalAuthenticationServiceImpl globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementServiceImpl globalSessionManagementService;

    @Autowired
    private GlobalUserManagementServiceImpl globalUserManagementService;

    private Ticket ticket = null;
    
    private static String tenant;

    @BeforeClass(groups = "deployment", enabled = true)
    public void setup() throws Exception {
        ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        assertEquals(ticket.getTenants().size(), 2);
        assertNotNull(ticket);
        createUser("rgonzalez", "rgonzalez@lattice-engines.com", "Ron", "Gonzalez");
        createUser("bnguyen", "bnguyen@lattice-engines.com", "Everything", "IsAwesome", "mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=");
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();
        tenant = tenant2;
        revokeRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "rgonzalez");
        revokeRight(GrantedRight.VIEW_PLS_REPORTING, tenant2, "bnguyen");
        revokeRight(GrantedRight.VIEW_PLS_MODELS, tenant2, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant1, "rgonzalez");
        grantRight(GrantedRight.VIEW_PLS_REPORTING, tenant2, "bnguyen");
        grantRight(GrantedRight.VIEW_PLS_MODELS, tenant2, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant2, "bnguyen");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant1, "admin");
        grantRight(GrantedRight.EDIT_PLS_USERS, tenant2, "admin");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant1, "admin");
        grantRight(GrantedRight.EDIT_PLS_MODELS, tenant2, "admin");

        setupDb(tenant1, tenant2, false);
        
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
        UserDocument doc = loginAndAttach("bnguyen", "tahoe");
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
