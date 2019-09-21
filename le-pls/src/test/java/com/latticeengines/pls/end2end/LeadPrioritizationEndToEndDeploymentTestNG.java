package com.latticeengines.pls.end2end;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.ModelingServiceExecutor;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBaseDeprecated;
import com.latticeengines.security.exposed.AccessLevel;

/**
 * This test needs access to remote Modeling Service API and HDP cluster.
 */
public class LeadPrioritizationEndToEndDeploymentTestNG extends PlsDeploymentTestNGBaseDeprecated {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeadPrioritizationEndToEndDeploymentTestNG.class);

    @Value("${common.test.microservice.url}")
    private String modelingServiceHostPort;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Deprecated
    @Value("${pls.modelingservice.testdsdb}")
    private String dataSourceDb;

    @Deprecated
    @Value("${pls.modelingservice.testdsdbhost}")
    private String dataSourceHost;

    @Deprecated
    @Value("${pls.modelingservice.testdsdbuser}")
    private String dataSourceUser;

    @Deprecated
    @Value("${pls.modelingservice.testdsdbpasswd.encrypted}")
    private String dataSourcePassword;

    @Deprecated
    @Value("${pls.modelingservice.testdsdbport}")
    private int dataSourcePort;

    @Autowired
    private Configuration yarnConfiguration;

    private static String tenant;
    private static Tenant tenantToAttach;

    @BeforeClass(groups = { "lpe2e", "deployment.production" })
    public void setup() throws Exception {
        deleteAndCreateTwoTenants();
        setupTestEnvironment();

        tenantToAttach = testingTenants.get(1);
        if (tenantToAttach.getName().contains("Tenant 1")) {
            tenantToAttach = testingTenants.get(0);
        }
        tenant = tenantToAttach.getId();
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(String.format("%s/%s", modelingServiceHdfsBaseDir, tenant)), true);
    }

    private void deleteAndCreateTwoTenants() throws Exception {
        turnOffSslChecking();
        setTestingTenants();
        for (Tenant tenant : testingTenants) {
            deleteTenantByRestCall(tenant.getId());
            createTenantByRestCall(tenant);
        }
    }

    @Deprecated
    private ModelingServiceExecutor buildModel(String tenant, String modelName, String metadata, String table)
            throws Exception {
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

        return new ModelingServiceExecutor(bldr);
    }

    @Test(groups = { "lpe2e", "deployment.production" }, enabled = false, dataProvider = "modelMetadataProvider")
    public void runPipeline(String tenant, String modelName, String metadataSuffix, String tableName) throws Exception {
        LOGGER.info(String.format("Running pipeline for model %s in tenant %s using table %s", modelName, tenant,
                tableName));
        ModelingServiceExecutor executor = buildModel(tenant, modelName, metadataSuffix, tableName);
        executor.init();
        executor.runPipeline();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "lpe2e", "deployment.production" }, enabled = false, dependsOnMethods = { "runPipeline" })
    public void checkModels() throws InterruptedException {
        UserDocument doc = loginAndAttach(AccessLevel.SUPER_ADMIN, tenantToAttach);
        useSessionDoc(doc);
        restTemplate.setErrorHandler(statusErrorHandler);
        int numOfRetries = 120;
        List response;
        do {
            response = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/", List.class);
            numOfRetries--;
            Thread.sleep(1000L);
        } while (numOfRetries > 0 && (response == null || response.size() < 2));
        if (numOfRetries <= 0) {
            LOGGER.warn("Probably exited the loop during out of number of retries.");
        }
        Assert.assertNotNull(response);
        Assert.assertTrue(response.size() >= 2,
                String.format("There is only %d models in tenant %s", response.size(), tenant));
        Map<String, String> map = (Map) response.get(0);
        ModelSummary summary = restTemplate.getForObject(getRestAPIHostPort() + "/pls/modelsummaries/" + map.get("Id"),
                ModelSummary.class);
        Assert.assertTrue(summary.getName().startsWith("PLSModel-Eloqua"));
        Assert.assertNotNull(summary.getDetails());
    }

    @DataProvider(name = "modelMetadataProvider")
    public static Object[][] getModelMetadataProvider() {
        return new Object[][] { //
                { tenant, "PLSModel-Eloqua1", "eloqua1", "Q_PLS_Modeling_Tenant1" }, //
                { tenant, "PLSModel-Eloqua2", "eloqua2", "Q_PLS_Modeling_Tenant2" } };
    }
}
