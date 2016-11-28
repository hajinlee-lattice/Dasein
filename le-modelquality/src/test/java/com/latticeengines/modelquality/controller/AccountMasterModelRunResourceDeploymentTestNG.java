package com.latticeengines.modelquality.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class AccountMasterModelRunResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    private static Log log = LogFactory.getLog(AccountMasterModelRunResourceDeploymentTestNG.class);

    private String user = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
    private String password = TestFrameworkUtils.GENERAL_PASSWORD;

    @Value("${modelquality.test.tenant:Model_Quality_Test_DnB}")
    protected String tenantName;

    private List<String> namedModelRunEntityNames = new ArrayList<>();
    private List<String> namedAnalyticPipelineEntityNames = new ArrayList<>();
    private List<String> allPropDataConfigNames = new ArrayList<>();
    private List<String> allDatasetNames = new ArrayList<>();

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        namedModelRunEntityNames.add("ModelQualityDeploymentTest-AccountMaster");
        namedModelRunEntityNames.add("ModelQualityDeploymentTest-DerivedColumn");

        namedAnalyticPipelineEntityNames.add("AccountMasterModelRunResourceDeploymentTestNG");
        namedAnalyticPipelineEntityNames.add("DerivedColumnModelRunResourceDeploymentTestNG");

        deleteLocalEntities();
        super.setup();
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, null);
    }

    @Override
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        deleteLocalEntities();
        super.tearDown();
    }

    @Test(groups = "deployment", enabled = true, dataProvider = "getAccountMasterCsvFile")
    public void runModelAccountMaster(String dataSetName, String csvFile) {
        try {
            ModelRunEntityNames aModelRunEntityNames = modelRunEntityNames.get(0);
            aModelRunEntityNames.setName(namedModelRunEntityNames.get(0));

            AnalyticPipelineEntityNames analyticPipelineEntityNames = modelQualityProxy
                    .getAnalyticPipelineByName(aModelRunEntityNames.getAnalyticPipelineName());
            analyticPipelineEntityNames.setName(namedAnalyticPipelineEntityNames.get(0));

            DataSet thisDataset = modelQualityProxy.getDataSetByName(dataset.getName());
            thisDataset.setName(dataSetName);
            thisDataset.setTenant(mainTestTenant);
            thisDataset.setTrainingSetHdfsPath( //
                    "/Pods/Default/Services/ModelQuality/" + csvFile);
            DataSet datasetAlreadyExists = dataSetEntityMgr.findByName(thisDataset.getName());
            if (datasetAlreadyExists != null)
                dataSetEntityMgr.delete(datasetAlreadyExists);
            modelQualityProxy.createDataSet(thisDataset);
            allDatasetNames.add(thisDataset.getName());

            PropData thisPropData = modelQualityProxy.getPropDataConfigByName(propData.getName());
            thisPropData.setName("ModelQualityDeploymentTest-AccountMaster");
            thisPropData.setDataCloudVersion("2.0.1");
            thisPropData.setExcludePublicDomains(true);
            PropData propDataAlreadyExists = propDataEntityMgr.findByName(thisPropData.getName());
            if (propDataAlreadyExists != null)
                propDataEntityMgr.delete(propDataAlreadyExists);
            modelQualityProxy.createPropDataConfig(thisPropData);
            allPropDataConfigNames.add(thisPropData.getName());

            analyticPipelineEntityNames.setPropData(thisPropData.getName());
            modelQualityProxy.createAnalyticPipeline(analyticPipelineEntityNames);

            aModelRunEntityNames.setAnalyticPipelineName(analyticPipelineEntityNames.getName());
            aModelRunEntityNames.setDataSetName(thisDataset.getName());
            System.out.println("Tenant=" + user + " Dataset=" + dataSetName);
            String modelName = modelQualityProxy.createModelRun(aModelRunEntityNames, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertEquals(aModelRunEntityNames.getName(), modelName);
            waitAndCheckModelRun(modelName);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", enabled = false, dataProvider = "getDerivedColumnCsvFile")
    public void runModelDerivedColumn(String dataSetName, String csvFile) {
        try {
            ModelRunEntityNames aModelRunEntityNames = modelRunEntityNames.get(0);
            aModelRunEntityNames.setName(namedModelRunEntityNames.get(1));

            AnalyticPipelineEntityNames analyticPipelineEntityNames = modelQualityProxy
                    .getAnalyticPipelineByName(aModelRunEntityNames.getAnalyticPipelineName());
            analyticPipelineEntityNames.setName(namedAnalyticPipelineEntityNames.get(1));

            DataSet thisDataset = modelQualityProxy.getDataSetByName(dataset.getName());
            thisDataset.setName(dataSetName);
            thisDataset.setTrainingSetHdfsPath( //
                    "/Pods/Default/Services/ModelQuality/" + csvFile);
            DataSet datasetAlreadyExists = dataSetEntityMgr.findByName(thisDataset.getName());
            if (datasetAlreadyExists != null)
                dataSetEntityMgr.delete(datasetAlreadyExists);
            modelQualityProxy.createDataSet(thisDataset);
            allDatasetNames.add(thisDataset.getName());

            PropData thisPropData = modelQualityProxy.getPropDataConfigByName(propData.getName());
            thisPropData.setName("ModelQualityDeploymentTest-DerivedColumn");
            thisPropData.setExcludePublicDomains(true);
            PropData propDataAlreadyExists = propDataEntityMgr.findByName(thisPropData.getName());
            if (propDataAlreadyExists != null)
                propDataEntityMgr.delete(propDataAlreadyExists);
            modelQualityProxy.createPropDataConfig(thisPropData);
            allPropDataConfigNames.add(thisPropData.getName());

            analyticPipelineEntityNames.setPropData(thisPropData.getName());
            modelQualityProxy.createAnalyticPipeline(analyticPipelineEntityNames);

            aModelRunEntityNames.setAnalyticPipelineName(analyticPipelineEntityNames.getName());
            aModelRunEntityNames.setDataSetName(dataset.getName());
            log.info("Tenant=" + user + " Dataset=" + dataSetName);
            String modelName = modelQualityProxy.createModelRun(aModelRunEntityNames, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertEquals(aModelRunEntityNames.getName(), modelName);
            waitAndCheckModelRun(modelName);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @DataProvider(name = "getAccountMasterCsvFile")
    public Object[][] getAccountMasterCsvFile() {
        return new Object[][] { //
                { "Mulesoft_NA_doman_AccountMaster", "Mulesoft_NA_domain.csv" }, //
                // { "Mulesoft_Emea_doman_AccountMaster",
                // "Mulesoft_Emea_domain.csv" }, //
                // { "Mulesoft_Apac_doman_AccountMaster",
                // "Mulesoft_Apac_domain.csv" }, //
                // { "Qlik_doman_AccountMaster", "Qlik_domaiin.csv" }, //
                // { "HootSuite_domain_AccountMaster", "HootSuite_domain.csv" },
                // //
                // { "CornerStone_domain_AccountMaster",
                // "CornerStone_domain.csv" }, //
                // { "PolyCom_domain_AccountMaster", "PolyCom_domain.csv" }, //
                // { "Tenable_domain_AccountMaster", "Tenable_domain.csv" }, //
                // { "bams_domain", "bams_domain.csv" }, //
        };
    }

    @DataProvider(name = "getDerivedColumnCsvFile")
    public Object[][] getAccountDerivedColumnCsvFile() {
        return new Object[][] {
        // { "Mulesoft_NA_doman_derived", "Mulesoft_NA_domain.csv" }, //
        // { "Mulesoft_Emea_doman_derived", "Mulesoft_Emea_domain.csv"
        // }, //
        // { "Mulesoft_Apac_doman_derived", "Mulesoft_Apac_domain.csv"
        // }, //
        // { "Qlik_doman_derived", "Qlik_domaiin.csv" }, //
        { "HootSuite_domain_derived", "HootSuite_domain.csv" }, //
        // { "CornerStone_domain_derived", "CornerStone_domain.csv" },
        // { "PolyCom_domain_derived", "PolyCom_domain.csv" }, //
        // { "Tenable_domain_derived", "Tenable_domain.csv" }, //
        };
    }

    private void deleteLocalEntities() {
        for (String name : namedModelRunEntityNames) {
            ModelRun modelRun = modelRunEntityMgr.findByName(name);
            if (modelRun != null) {
                modelRunEntityMgr.delete(modelRun);
            }
        }

        for (String name : namedAnalyticPipelineEntityNames) {
            AnalyticPipeline analyticPipeline = analyticPipelineEntityMgr.findByName(name);
            if (analyticPipeline != null) {
                analyticPipelineEntityMgr.delete(analyticPipeline);
            }
        }

        for (String name : allPropDataConfigNames) {
            PropData retrievedPropData = propDataEntityMgr.findByName(name);
            if (retrievedPropData != null) {
                propDataEntityMgr.delete(retrievedPropData);
            }
        }

        for (String name : allDatasetNames) {
            DataSet retrievedDataset = dataSetEntityMgr.findByName(name);
            if (retrievedDataset != null) {
                dataSetEntityMgr.delete(retrievedDataset);
            }
        }
    }

}
