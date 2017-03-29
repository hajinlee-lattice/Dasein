package com.latticeengines.modelquality.controller;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class ModelRunResourceDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    private String user = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
    private String password = TestFrameworkUtils.GENERAL_PASSWORD;
    private List<String> namedModelRunEntityNames = new ArrayList<>();
    private List<String> allDatasetNames = new ArrayList<>();

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        namedModelRunEntityNames.add("ModelQualityDeploymentTest-Lead");
        namedModelRunEntityNames.add("ModelQualityDeploymentTest-LeadNewPipeline");
        namedModelRunEntityNames.add("ModelQualityDeploymentTest-Account");
        deleteLocalEntities();
        super.setup();
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, null, null);
    }

    @Override
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        deleteLocalEntities();
        super.tearDown();
    }

    @Test(groups = "deployment", enabled = true)
    public void runModelNGINX() {
        try {
            ModelRunEntityNames aModelRunEntityNames = modelRunEntityNames.get(0);
            aModelRunEntityNames.setName(namedModelRunEntityNames.get(0));

            DataSet thisDataset = modelQualityProxy.getDataSetByName(dataset.getName());
            thisDataset.setName("ModelRunResourceDeploymentTestNG-NGINX");
            thisDataset.setTrainingSetHdfsPath(
                    "/Pods/Default/Services/ModelQuality/datasets/NGINXReducedRowsEnhanced_20160712.csv");
            thisDataset.setSchemaInterpretation(SchemaInterpretation.SalesforceLead);
            DataSet datasetAlreadyExists = dataSetEntityMgr.findByName(thisDataset.getName());
            if (datasetAlreadyExists != null)
                dataSetEntityMgr.delete(datasetAlreadyExists);
            modelQualityProxy.createDataSet(thisDataset);
            allDatasetNames.add(thisDataset.getName());

            aModelRunEntityNames.setDataSetName(thisDataset.getName());
            String modelName = modelQualityProxy.createModelRun(aModelRunEntityNames, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertEquals(aModelRunEntityNames.getName(), modelName);
            waitAndCheckModelRun(modelName);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", enabled = false)
    public void runModelNGINXNewPipeline() {
        try {
            ModelRunEntityNames aModelRunEntityNames = modelRunEntityNames.get(1);
            aModelRunEntityNames.setName(namedModelRunEntityNames.get(1));

            DataSet thisDataset = modelQualityProxy.getDataSetByName(dataset.getName());
            thisDataset.setName("ModelRunResourceDeploymentTestNG-NGINX-ForNewPipeline");
            thisDataset.setTrainingSetHdfsPath(
                    "/Pods/Default/Services/ModelQuality/NGINX_PLS_LP3_ModelingLead_ReducedRows_20160712_125224.csv");
            DataSet datasetAlreadyExists = dataSetEntityMgr.findByName(thisDataset.getName());
            if (datasetAlreadyExists != null)
                dataSetEntityMgr.delete(datasetAlreadyExists);
            modelQualityProxy.createDataSet(thisDataset);
            allDatasetNames.add(thisDataset.getName());

            aModelRunEntityNames.setDataSetName(thisDataset.getName());
            String modelName = modelQualityProxy.createModelRun(aModelRunEntityNames, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertEquals(aModelRunEntityNames.getName(), modelName);
            waitAndCheckModelRun(modelName);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "runModelNGINXNewPipeline", enabled = false)
    public void runModelHosting() {
        try {
            ModelRunEntityNames aModelRunEntityNames = modelRunEntityNames.get(0);
            aModelRunEntityNames.setName(namedModelRunEntityNames.get(2));

            DataSet thisDataset = modelQualityProxy.getDataSetByName(dataset.getName());
            thisDataset.setName("ModelRunResourceDeploymentTestNG-Hosting");
            thisDataset.setTrainingSetHdfsPath(
                    "/Pods/Default/Services/ModelQuality/hostingcom/hostingcom_rowsremoved.csv");
            thisDataset.setSchemaInterpretation(SchemaInterpretation.SalesforceAccount);
            DataSet datasetAlreadyExists = dataSetEntityMgr.findByName(thisDataset.getName());
            if (datasetAlreadyExists != null)
                dataSetEntityMgr.delete(datasetAlreadyExists);
            modelQualityProxy.createDataSet(thisDataset);
            allDatasetNames.add(thisDataset.getName());

            aModelRunEntityNames.setDataSetName(thisDataset.getName());
            String modelName = modelQualityProxy.createModelRun(aModelRunEntityNames, //
                    mainTestTenant.getId(), user, password, plsDeployedHostPort);
            Assert.assertEquals(aModelRunEntityNames.getName(), modelName);
            waitAndCheckModelRun(modelName);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Failed", ex);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "runModelNGINX", enabled = true)
    public void getModelRuns() {
        try {
            modelQualityProxy.getModelRuns();
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    private void deleteLocalEntities() {
        for (String name : namedModelRunEntityNames) {
            ModelRun modelRun = modelRunEntityMgr.findByName(name);
            if (modelRun != null) {
                modelRunEntityMgr.delete(modelRun);
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
