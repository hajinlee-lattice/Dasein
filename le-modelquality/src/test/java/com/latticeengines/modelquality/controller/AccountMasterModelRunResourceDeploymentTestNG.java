package com.latticeengines.modelquality.controller;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class AccountMasterModelRunResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    private String user = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
    private String password = TestFrameworkUtils.GENERAL_PASSWORD;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        // modelRunEntityMgr.deleteAll();
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
    }

    @Test(groups = "deployment", enabled = false, dataProvider = "getAccountMasterCsvFile")
    public void runModelAccountMaster(String dataSetName, String csvFile) {
        try {
            ModelRun modelRun = createModelRuns().get(0);
            modelRun.getSelectedConfig().getDataSet().setName(dataSetName);
            modelRun.getSelectedConfig().getDataSet().setTrainingSetHdfsPath( //
                    "/Pods/Default/Services/ModelQuality/" + csvFile);
            modelRun.getSelectedConfig().getPropData().setDataCloudVersion("2.0.0");

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
                { "MuleSoftAccountMaster", "Mulesoft_MKTO_LP3_ModelingLead_OneLeadPerDomain_NA_20160808_135904.csv" }, //
                { "QlikAccountMaster", "Qlik_Migration_LP3_ModelingLead_OneLeadPerDomain_20160808_171437.csv" }, //
                { "LatticeAccountMaster", "Lattice_Relaunch_LP3_ModelingLead_OneLeadPerDomain_20160520_161932.csv" }, //
                { "HootSuiteAccountMaster",
                        "HootSuite_PLS132_Clone_LP3_ModelingLead_OneLeadPerDomain_OrigAct_20160520_161631.csv" }, //
        };
    }

    @DataProvider(name = "getDerivedColumnCsvFile")
    public Object[][] getAccountDerivedColumnCsvFile() {
        return new Object[][] {
                { "MuleSoftDerivedColumn", "Mulesoft_MKTO_LP3_ModelingLead_OneLeadPerDomain_NA_20160808_135904.csv" }, //
                { "QlikDerivedColumn", "Qlik_Migration_LP3_ModelingLead_OneLeadPerDomain_20160808_171437.csv" }, //
                { "LatticeDerivedColumn", "Lattice_Relaunch_LP3_ModelingLead_OneLeadPerDomain_20160520_161932.csv" }, //
                { "HootSuiteDerivedColumn",
                        "HootSuite_PLS132_Clone_LP3_ModelingLead_OneLeadPerDomain_OrigAct_20160520_161631.csv" }, //
        };
    }
}
