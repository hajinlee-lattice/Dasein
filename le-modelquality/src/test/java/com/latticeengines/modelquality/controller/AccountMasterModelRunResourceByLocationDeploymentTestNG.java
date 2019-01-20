package com.latticeengines.modelquality.controller;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;

public class AccountMasterModelRunResourceByLocationDeploymentTestNG extends BaseAccountMasterModelRunDeploymentTestNG {

    private static final ImmutableMap<String, String> testCases = ImmutableMap.<String, String>builder() //
            .put("Mulesoft_NA_loc_AccountMaster", "Mulesoft_NA_loc.csv") //
            .put("Mulesoft_Emea_loc_AccountMaster", "Mulesoft_Emea_loc.csv") //
            .put("Mulesoft_Apac_loc_AccountMaster", "Mulesoft_apac_loc.csv") //
            .put("Qlik_loc_AccountMaster", "Qlik_loc.csv") //
            .put("HootSuite_loc_AccountMaster", "HootSuite_loc.csv") //
            .put("CornerStone_loc_AccountMaster", "Corner_loc.csv") //
            .put("PolyCom_loc_AccountMaster", "PolyCom_loc.csv") //
            .put("Tenable_loc_AccountMaster", "Tenable_loc.csv") //
            .build();

    @SuppressWarnings("deprecation")
    @Override
    @BeforeClass(groups = { "deployment", "am", "am_all" })
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
        featureFlagMap.put(LatticeFeatureFlag.BYPASS_DNB_CACHE.getName(), false);

        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, null, featureFlagMap);
    }

    @Override
    @AfterClass(groups = { "deployment", "am", "am_all" })
    public void tearDown() throws Exception {
        deleteLocalEntities();
        super.tearDown();
    }

    @Test(groups = "am")
    public void runModelForOneCsv() {
        String dataSetName = getSystemProperty("MQ_DATASET");
        if (testCases.containsKey(dataSetName)) {
            String csvFile = testCases.get(dataSetName);
            runModelAccountMaster(dataSetName, csvFile);
        } else {
            logger.info(String.format("Skipping run model, dataSetName=%s", dataSetName));
        }
    }

    @Test(groups = { "am_all" }, dataProvider = "getAccountMasterLocationCsvFile")
    public void runModelAccountMasterLocation(String dataSetName, String csvFile) {
        runModelAccountMaster(dataSetName, csvFile);
    }

    @DataProvider(name = "getAccountMasterLocationCsvFile")
    public Object[][] getAccountMasterLocationCsvFile() {
        ImmutableList<Map.Entry<String, String>> list = testCases.entrySet().asList();
        Object[][] data = new Object[list.size()][2];
        for (int i = 0; i < list.size(); i++) {
            Map.Entry<String, String> entry = list.get(i);
            data[i] = new Object[] { entry.getKey(), entry.getValue() };
        }
        return data;
    }
}
