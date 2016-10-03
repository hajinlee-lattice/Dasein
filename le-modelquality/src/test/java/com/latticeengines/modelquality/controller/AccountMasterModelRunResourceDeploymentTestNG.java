package com.latticeengines.modelquality.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class AccountMasterModelRunResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    private static Log log = LogFactory.getLog(AccountMasterModelRunResourceDeploymentTestNG.class);

    private String user = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
    private String password = TestFrameworkUtils.GENERAL_PASSWORD;

    @BeforeMethod(groups = "deployment")
    public void setup() throws Exception {
        super.cleanupDb();
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment", enabled = true, dataProvider = "getAccountMasterCsvFile")
    public void runModelAccountMaster(String dataSetName, String csvFile) {
        try {
            ModelRun modelRun = createModelRuns().get(0);
            modelRun.getSelectedConfig().getDataSet().setName(dataSetName);
            modelRun.getSelectedConfig().getDataSet().setTrainingSetHdfsPath( //
                    "/Pods/Default/Services/ModelQuality/" + csvFile);
            modelRun.getSelectedConfig().getPropData().setDataCloudVersion("2.0.0");
            modelRun.getSelectedConfig().getPropData().setExcludePublicDomains(true);

            log.info("Tenant=" + user + " Dataset=" + dataSetName);
            ResponseDocument<String> response = modelQualityProxy.runModel(modelRun, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertTrue(response.isSuccess());

            String modelRunId = response.getResult();
            waitAndCheckModelRun(modelRunId);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", enabled = false, dataProvider = "getDerivedColumnCsvFile")
    public void runModelDerivedColumn(String dataSetName, String csvFile) {
        try {
            ModelRun modelRun = createModelRuns().get(0);
            modelRun.getSelectedConfig().getDataSet().setName(dataSetName);
            modelRun.getSelectedConfig().getDataSet().setTrainingSetHdfsPath( //
                    "/Pods/Default/Services/ModelQuality/" + csvFile);
            modelRun.getSelectedConfig().getPropData().setExcludePublicDomains(true);

            log.info("Tenant=" + user + " Dataset=" + dataSetName);
            ResponseDocument<String> response = modelQualityProxy.runModel(modelRun, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertTrue(response.isSuccess());

            String modelRunId = response.getResult();
            waitAndCheckModelRun(modelRunId);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @DataProvider(name = "getAccountMasterCsvFile")
    public Object[][] getAccountMasterCsvFile() {
        return new Object[][] {
        // { "Mulesoft_NA_doman_AccountMaster", "Mulesoft_NA_domain.csv" }, //
        { "bams_domain", "bams_domain.csv" }, //
        // { "Mulesoft_Emea_doman_AccountMaster", "Mulesoft_Emea_domain.csv" },
        // //
        // { "Mulesoft_Apac_doman_AccountMaster", "Mulesoft_Apac_domain.csv" },
        // //
        // { "Qlik_doman_AccountMaster", "Qlik_domaiin.csv" }, //
        // { "HootSuite_domain_AccountMaster",
        // "HootSuite_PLS132_Clone_LP3_ModelingLead_OneLeadPerDomain_OrigAct_20160520_161631.csv"
        // },
        // //
        // { "CornerStone_domain_AccountMaster",
        // "CornerStone_domain.csv" }, //
        // { "PolyCom_domain_AccountMaster", "PolyCom_domain.csv" }, //
        // { "Tenable_domain_AccountMaster", "Tenable_domain.csv" }, //

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
