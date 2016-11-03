package com.latticeengines.modelquality.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class AccountMasterModelRunResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    private static Log log = LogFactory.getLog(AccountMasterModelRunResourceDeploymentTestNG.class);

    private String user = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
    private String password = TestFrameworkUtils.GENERAL_PASSWORD;

    @Value("${modelquality.test.tenant:Model_Quality_Test_DnB}")
    protected String tenantName;

    @BeforeMethod(groups = "deployment")
    public void setup() throws Exception {
        super.cleanupDb();
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, tenantName);
    }

    @Test(groups = "deployment", enabled = true, dataProvider = "getAccountMasterCsvFile")
    public void runModelAccountMaster(String dataSetName, String csvFile) {
        try {
            ModelRun modelRun = createModelRuns().get(0);
            modelRun.getDataSet().setName(dataSetName);
            modelRun.getDataSet().setTrainingSetHdfsPath( //
                    "/Pods/Default/Services/ModelQuality/" + csvFile);
            modelRun.getAnalyticPipeline().getPropData().setDataCloudVersion("2.0.1");
            modelRun.getAnalyticPipeline().getPropData().setExcludePublicDomains(true);

            dataSetEntityMgr.create(modelRun.getDataSet());
            propDataEntityMgr.update(modelRun.getAnalyticPipeline().getPropData());
            ModelRunEntityNames modelRunEntityNames = new ModelRunEntityNames(modelRun);

            log.info("Tenant=" + user + " Dataset=" + dataSetName);
            String modelName = modelQualityProxy.createModelRun(modelRunEntityNames, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertEquals(modelRun.getName(), modelName);
            waitAndCheckModelRun(modelName);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", enabled = false, dataProvider = "getDerivedColumnCsvFile")
    public void runModelDerivedColumn(String dataSetName, String csvFile) {
        try {
            ModelRun modelRun = createModelRuns().get(0);
            modelRun.getDataSet().setName(dataSetName);
            modelRun.getDataSet().setTrainingSetHdfsPath( //
                    "/Pods/Default/Services/ModelQuality/" + csvFile);
            modelRun.getAnalyticPipeline().getPropData().setExcludePublicDomains(true);
            dataSetEntityMgr.create(modelRun.getDataSet());

            ModelRunEntityNames modelRunEntityNames = new ModelRunEntityNames(modelRun);

            log.info("Tenant=" + user + " Dataset=" + dataSetName);
            String modelName = modelQualityProxy.createModelRun(modelRunEntityNames, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertEquals(modelRun.getName(), modelName);
            waitAndCheckModelRun(modelName);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @DataProvider(name = "getAccountMasterCsvFile")
    public Object[][] getAccountMasterCsvFile() {
        return new Object[][] {

        //
        { "Mulesoft_NA_doman_AccountMaster", "Mulesoft_NA_domain.csv" }, //
        // { "Mulesoft_Emea_doman_AccountMaster", "Mulesoft_Emea_domain.csv" }, //
        // { "Mulesoft_Apac_doman_AccountMaster", "Mulesoft_Apac_domain.csv" }, //
        // { "Qlik_doman_AccountMaster", "Qlik_domaiin.csv" }, //
        // { "HootSuite_domain_AccountMaster", "HootSuite_domain.csv" }, //
        // { "CornerStone_domain_AccountMaster", "CornerStone_domain.csv" }, //
        // { "PolyCom_domain_AccountMaster", "PolyCom_domain.csv" }, //
        // { "Tenable_domain_AccountMaster", "Tenable_domain.csv" }, //

        // { "bams_domain", "bams_domain.csv" }, //
        // { "Tenable_domain_clean_alexa_AccountMaster",
        // "Tenable_domain_clean_alexa.csv" }, //
        // { "PolyCom_domain_clean_alexa_AccountMaster",

        };
    }

    @DataProvider(name = "getDerivedColumnCsvFile")
    public Object[][] getAccountDerivedColumnCsvFile() {
        return new Object[][] {
                { "Mulesoft_NA_doman_derived", "Mulesoft_NA_domain.csv" }, //
                { "Mulesoft_Emea_doman_derived", "Mulesoft_Emea_domain.csv" }, //
                { "Mulesoft_Apac_doman_derived", "Mulesoft_Apac_domain.csv" }, //
                { "Qlik_doman_derived", "Qlik_domaiin.csv" }, //
                { "HootSuite_domain_derived", "HootSuite_domain.csv" }, //
                { "CornerStone_domain_derived", "CornerStone_domain.csv" },
                { "PolyCom_domain_derived", "PolyCom_domain.csv" }, //
                { "Tenable_domain_derived", "Tenable_domain.csv" }, //
        };
    }

}
