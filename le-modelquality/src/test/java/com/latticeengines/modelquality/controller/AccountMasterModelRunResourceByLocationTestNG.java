package com.latticeengines.modelquality.controller;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;

public class AccountMasterModelRunResourceByLocationTestNG extends BaseAccountMasterModelRunDeploymentTestNG {

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        namedModelRunEntityNames.add("ModelQualityDeploymentTest-AccountMaster");
        namedModelRunEntityNames.add("ModelQualityDeploymentTest-DerivedColumn");

        namedAnalyticPipelineEntityNames.add("AccountMasterModelRunResourceDeploymentTestNG");
        namedAnalyticPipelineEntityNames.add("DerivedColumnModelRunResourceDeploymentTestNG");

        deleteLocalEntities();
        super.setup();

        Map<String, Boolean> featureFlagMap = new HashMap<String, Boolean>();
        featureFlagMap.put(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName(), true);
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName(), true);

        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, null, featureFlagMap);
    }

    @Override
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        deleteLocalEntities();
        super.tearDown();
    }

    @Test(groups = "deployment", enabled = true, dataProvider = "getAccountMasterLocationCsvFile")
    public void runModelAccountMasterLoction(String dataSetName, String csvFile) {
        runModelAccountMaster(dataSetName, csvFile);
    }

    @DataProvider(name = "getAccountMasterLocationCsvFile")
    public Object[][] getAccountMasterLocationCsvFile() {
        return new Object[][] { //
                { "Mulesoft_NA_loc_AccountMaster", "Mulesoft_NA_loc.csv" }, //
                { "Mulesoft_Emea_loc_AccountMaster", "Mulesoft_Emea_loc.csv" }, //
                { "Mulesoft_Apac_loc_AccountMaster", "Mulesoft_apac_loc.csv" }, //
                { "Qlik_loc_AccountMaster", "Qlik_loc.csv" }, //
                { "HootSuite_loc_AccountMaster", "HootSuite_loc.csv" }, //
                { "CornerStone_loc_AccountMaster", "Corner_loc.csv" }, //
                { "PolyCom_loc_AccountMaster", "PolyCom_loc.csv" }, //
                { "Tenable_loc_AccountMaster", "Tenable_loc.csv" }, //
        };
    }
}
