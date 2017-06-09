package com.latticeengines.modelquality.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.modelquality.functionalframework.ModelQualityDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class BaseAccountMasterModelRunDeploymentTestNG extends ModelQualityDeploymentTestNGBase {

    protected String user = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
    protected String password = TestFrameworkUtils.GENERAL_PASSWORD;

    @Value("${modelquality.test.tenant:Model_Quality_Test_DnB}")
    protected String tenantName;

    protected List<String> namedModelRunEntityNames = new ArrayList<>();
    protected List<String> namedAnalyticPipelineEntityNames = new ArrayList<>();
    protected List<String> allPropDataConfigNames = new ArrayList<>();
    protected List<String> allDatasetNames = new ArrayList<>();

    protected void runModelAccountMaster(String dataSetName, String csvFile) {
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
            thisDataset.setSchemaInterpretation(SchemaInterpretation.SalesforceLead);
            DataSet datasetAlreadyExists = dataSetEntityMgr.findByName(thisDataset.getName());
            if (datasetAlreadyExists != null)
                dataSetEntityMgr.delete(datasetAlreadyExists);
            modelQualityProxy.createDataSet(thisDataset);
            allDatasetNames.add(thisDataset.getName());

            PropData thisPropData = modelQualityProxy.getPropDataConfigByName(propData.getName());
            thisPropData.setName("ModelQualityDeploymentTest-AccountMaster");
            thisPropData.setDataCloudVersion("2.0.4");
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

    protected void deleteLocalEntities() {
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
