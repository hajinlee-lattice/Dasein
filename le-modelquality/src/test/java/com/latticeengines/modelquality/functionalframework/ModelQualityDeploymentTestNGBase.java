package com.latticeengines.modelquality.functionalframework;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.modelquality.service.AnalyticPipelineService;
import com.latticeengines.modelquality.service.ModelRunService;
import com.latticeengines.modelquality.service.impl.PipelineStepType;
import com.latticeengines.proxy.exposed.modelquality.ModelQualityProxy;
import com.latticeengines.testframework.security.impl.GlobalAuthDeploymentTestBed;

import edu.emory.mathcs.backport.java.util.Arrays;

public class ModelQualityDeploymentTestNGBase extends ModelQualityTestNGBase {

    @Autowired
    protected ModelQualityProxy modelQualityProxy;

    @Value("${common.test.pls.url}")
    protected String plsDeployedHostPort;

    @Value("${common.test.admin.url}")
    protected String adminDeployedHostPort;

    @Autowired
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    protected Tenant mainTestTenant;

    @Autowired
    private AnalyticPipelineService analyticPipelineService;

    @Resource(name = "modelRunService")
    protected ModelRunService modelRunService;

    protected Algorithm algorithm;
    protected DataFlow dataflow;
    protected PropData propData;
    protected Sampling sampling;
    protected Pipeline pipeline1;
    protected Pipeline pipeline2;
    protected DataSet dataset;

    protected final String analyticPipline2Name = "ModelQualityDeploymentTest-2";

    protected List<AnalyticPipeline> analyticPipelines = new ArrayList<>();
    protected AnalyticTest analyticTest;
    protected List<ModelRunEntityNames> modelRunEntityNames = new ArrayList<>();

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {

        String analyticTestStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/analytictest.json")
                        .getFile()));
        AnalyticTestEntityNames analyticTestEntityNames = JsonUtils.deserialize(analyticTestStr,
                AnalyticTestEntityNames.class);
        AnalyticTest analyticTestPrevAlreadyExists = analyticTestEntityMgr.findByName("TestAnalyticTest");
        if (analyticTestPrevAlreadyExists != null) {
            System.out.println(String.format("Attempting to delete AnalyticTest \"%s\"", "TestAnalyticTest"));
            analyticTestEntityMgr.delete(analyticTestPrevAlreadyExists);
        }
        AnalyticTest analyticTestAlreadyExists = analyticTestEntityMgr.findByName(analyticTestEntityNames.getName());
        if (analyticTestAlreadyExists != null) {
            System.out.println(
                    String.format("Attempting to delete AnalyticTest \"%s\"", analyticTestEntityNames.getName()));
            analyticTestEntityMgr.delete(analyticTestAlreadyExists);
        }

        String analyticPipelineStr = FileUtils.readFileToString(new File( //
                ClassLoader
                        .getSystemResource("com/latticeengines/modelquality/functionalframework/analyticpipeline.json")
                        .getFile()));
        AnalyticPipelineEntityNames analyticPipelineEntityNames = JsonUtils.deserialize(analyticPipelineStr,
                AnalyticPipelineEntityNames.class);

        List<ModelRun> existingModelRuns = modelRunEntityMgr.findAll();
        for (ModelRun aModelRun : existingModelRuns) {
            AnalyticPipeline anAnalyticPipeline = analyticPipelineEntityMgr
                    .findByName(aModelRun.getAnalyticPipeline().getName());
            if (anAnalyticPipeline.getName().equals(analyticPipelineEntityNames.getName()) //
                    || anAnalyticPipeline.getName().equals(analyticPipline2Name)) {
                System.out.println(String.format("Attempting to delete ModelRun \"%s\"", aModelRun.getName()));
                modelRunEntityMgr.delete(aModelRun);
            }
        }

        AnalyticPipeline analyticPipeline1AlreadyExists = analyticPipelineEntityMgr
                .findByName(analyticPipelineEntityNames.getName());
        if (analyticPipeline1AlreadyExists != null) {
            System.out.println(String.format("Attempting to delete AnalyticPipeline \"%s\"",
                    analyticPipelineEntityNames.getName()));
            analyticPipelineEntityMgr.delete(analyticPipeline1AlreadyExists);
        }
        AnalyticPipeline analyticPipeline2AlreadyExists = analyticPipelineEntityMgr.findByName(analyticPipline2Name);
        if (analyticPipeline2AlreadyExists != null) {
            System.out.println(String.format("Attempting to delete AnalyticPipeline \"%s\"", analyticPipline2Name));
            analyticPipelineEntityMgr.delete(analyticPipeline2AlreadyExists);
        }

        String algorithmStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/algorithm.json")
                        .getFile()));
        algorithm = JsonUtils.deserialize(algorithmStr, Algorithm.class);
        Algorithm algorithmAlreadyExists = algorithmEntityMgr.findByName(algorithm.getName());
        if (algorithmAlreadyExists == null) {
            algorithmEntityMgr.create(algorithm);
        }

        String dataflowStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/dataflow.json")
                        .getFile()));
        dataflow = JsonUtils.deserialize(dataflowStr, DataFlow.class);
        DataFlow dataFlowAlreadyExists = dataFlowEntityMgr.findByName(dataflow.getName());
        if (dataFlowAlreadyExists != null) {
            System.out.println(String.format("Attempting to delete DataFlow \"%s\"", dataflow.getName()));
            dataFlowEntityMgr.delete(dataFlowAlreadyExists);
        }
        dataFlowEntityMgr.create(dataflow);

        String propDataStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/propdata.json")
                        .getFile()));
        propData = JsonUtils.deserialize(propDataStr, PropData.class);
        PropData propDataAlreadyExists = propDataEntityMgr.findByName(propData.getName());
        if (propDataAlreadyExists != null) {
            System.out.println(String.format("Attempting to delete PropData \"%s\"", propData.getName()));
            propDataEntityMgr.delete(propDataAlreadyExists);
        }
        propDataEntityMgr.create(propData);

        String samplingStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/sampling.json")
                        .getFile()));
        sampling = JsonUtils.deserialize(samplingStr, Sampling.class);
        Sampling samplingAlreadyExists = samplingEntityMgr.findByName(sampling.getName());
        if (samplingAlreadyExists != null) {
            System.out.println(String.format("Attempting to delete Sampling \"%s\"", sampling.getName()));
            samplingEntityMgr.delete(samplingAlreadyExists);
        }
        samplingEntityMgr.create(sampling);

        PipelineStep pipelineStepAlreadyExists = pipelineStepEntityMgr.findByName("remediatedatarulesstep");
        if (pipelineStepAlreadyExists == null) {
            pipelineService.createLatestProductionPipeline();
        }
        pipelineStepAlreadyExists = pipelineStepEntityMgr.findByName("assigncategorical");
        if (pipelineStepAlreadyExists != null) {
            System.out.println("Attempting to delete PipelineStep \"assigncategorical\"");
            pipelineStepEntityMgr.delete(pipelineStepAlreadyExists);
        }

        String pipelineStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/pipeline.json")
                        .getFile()));
        pipeline1 = JsonUtils.deserialize(pipelineStr, Pipeline.class);
        Pipeline pipelineAlreadyExists = pipelineEntityMgr.findByName(pipeline1.getName());
        if (pipelineAlreadyExists != null) {
            System.out.println(String.format("Attempting to delete Pipeline \"%s\"", pipeline1.getName()));
            pipelineEntityMgr.delete(pipelineAlreadyExists);
        }

        List<PipelineStep> pipeline1Steps = pipeline1.getPipelineSteps();
        List<PipelineStepOrFile> pipeline1StepsOrFiles = new ArrayList<>();
        for (PipelineStep p : pipeline1Steps) {
            PipelineStepOrFile psof = new PipelineStepOrFile();
            psof.pipelineStepName = p.getName();
            pipeline1StepsOrFiles.add(psof);
        }

        pipeline1 = pipelineService.createPipeline(pipeline1.getName(), pipeline1.getDescription(),
                pipeline1StepsOrFiles);

        pipeline2 = createNewDataPipeline(pipeline1);

        String datasetStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/dataset.json")
                        .getFile()));
        dataset = JsonUtils.deserialize(datasetStr, DataSet.class);
        DataSet datasetAlreadyExists = dataSetEntityMgr.findByName(dataset.getName());
        if (datasetAlreadyExists != null) {
            System.out.println(String.format("Attempting to delete DataSet \"%s\"", dataset.getName()));
            dataSetEntityMgr.delete(datasetAlreadyExists);
        }
        dataSetEntityMgr.create(dataset);

        AnalyticPipeline analyticPipeline1 = analyticPipelineService
                .createAnalyticPipeline(analyticPipelineEntityNames);

        analyticPipelineEntityNames.setName(analyticPipline2Name);
        analyticPipelineEntityNames.setPipeline(pipeline2.getName());
        AnalyticPipeline analyticPipeline2 = analyticPipelineService
                .createAnalyticPipeline(analyticPipelineEntityNames);

        analyticPipelines.add(analyticPipeline1);
        analyticPipelines.add(analyticPipeline2);

        List<String> analyticPipelineNames = new ArrayList<>();
        analyticPipelineNames.add(analyticPipeline1.getName());
        analyticPipelineNames.add(analyticPipeline2.getName());
        List<String> datasetNames = new ArrayList<>();
        datasetNames.add(dataset.getName());
        analyticTestEntityNames.setAnalyticPipelineNames(analyticPipelineNames);
        analyticTestEntityNames.setDataSetNames(datasetNames);
        analyticTest = analyticTestService.createAnalyticTest(analyticTestEntityNames);

        String modelRunStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/modelrun.json")
                        .getFile()));
        ModelRunEntityNames modelRunEntityNames1 = JsonUtils.deserialize(modelRunStr, ModelRunEntityNames.class);
        modelRunEntityNames1.setAnalyticPipelineName(analyticPipeline1.getName());
        modelRunEntityNames1.setDataSetName(dataset.getName());
        modelRunEntityNames1.setName("ModelQualityDeploymentTest-1");
        ModelRunEntityNames modelRunEntityNames2 = JsonUtils.deserialize(modelRunStr, ModelRunEntityNames.class);
        modelRunEntityNames2.setAnalyticPipelineName(analyticPipeline2.getName());
        modelRunEntityNames2.setDataSetName(dataset.getName());
        modelRunEntityNames2.setName("ModelQualityDeploymentTest-2");

        modelRunEntityNames.add(modelRunEntityNames1);
        modelRunEntityNames.add(modelRunEntityNames2);
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        System.out.println(String.format("Attempting to delete AnalyticTest \"%s\"", analyticTest.getName()));
        analyticTestEntityMgr.delete(analyticTest);
        System.out.println(String.format("Attempting to delete DataSet \"%s\"", dataset.getName()));
        dataSetEntityMgr.delete(dataset);
        System.out.println(String.format("Attempting to delete Pipeline \"%s\"", pipeline1.getName()));
        pipelineEntityMgr.delete(pipeline1);
        System.out.println(String.format("Attempting to delete Pipeline \"%s\"", pipeline2.getName()));
        pipelineEntityMgr.delete(pipeline2);
        PipelineStep pipelineStepAlreadyExists = pipelineStepEntityMgr.findByName("assigncategorical");
        if (pipelineStepAlreadyExists != null) {
            System.out.println("Attempting to delete PipelineStep \"assigncategorical\"");
            pipelineStepEntityMgr.delete(pipelineStepAlreadyExists);
        }
        System.out.println(String.format("Attempting to delete Sampling \"%s\"", sampling.getName()));
        samplingEntityMgr.delete(sampling);
        System.out.println(String.format("Attempting to delete PropData \"%s\"", propData.getName()));
        propDataEntityMgr.delete(propData);
        System.out.println(String.format("Attempting to delete DataFlow \"%s\"", dataflow.getName()));
        dataFlowEntityMgr.delete(dataflow);
    }

    protected void setTestBed(GlobalAuthDeploymentTestBed testBed) {
        this.deploymentTestBed = testBed;
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product, String tenantName)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        deploymentTestBed.bootstrapForProduct(product);
        if (tenantName == null) {
            mainTestTenant = deploymentTestBed.getMainTestTenant();
        } else {
            mainTestTenant = deploymentTestBed.addExtraTestTenant(tenantName);
        }
        switchToSuperAdmin();
    }

    protected void switchToSuperAdmin() {
        deploymentTestBed.switchToSuperAdmin(mainTestTenant);
    }

    @SuppressWarnings("unchecked")
    protected List<SelectedConfig> getSelectedConfigs() {
        SelectedConfig selectedConfig1 = new SelectedConfig();
        selectedConfig1.setAlgorithm(algorithm);
        selectedConfig1.setDataFlow(dataflow);
        selectedConfig1.setDataSet(dataset);
        selectedConfig1.setPipeline(pipeline1);
        selectedConfig1.setPropData(propData);
        selectedConfig1.setSampling(sampling);

        SelectedConfig selectedConfig2 = new SelectedConfig();
        selectedConfig2.setAlgorithm(algorithm);
        selectedConfig2.setDataFlow(dataflow);
        selectedConfig2.setDataSet(dataset);
        selectedConfig2.setPipeline(pipeline2);
        selectedConfig2.setPropData(propData);
        selectedConfig2.setSampling(sampling);

        return Arrays.asList(new SelectedConfig[] { selectedConfig1, selectedConfig2 });
    }

    protected String uploadPipelineStepFile(PipelineStepType type) throws Exception {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        org.springframework.core.io.Resource resource = new ClassPathResource(
                "com/latticeengines/modelquality/service/impl/assignconversionratetoallcategoricalvalues." //
                        + type.getFileExtension());
        ;
        switch (type) {
        case PYTHONRTS:
            resource = new ClassPathResource("com/latticeengines/modelquality/service/impl/assignconversionrate.py");
            break;
        default:
            break;
        }

        map.add("file", resource);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        String fileName = "assignconversionratetoallcategoricalvalues." + type.getFileExtension();

        switch (type) {
        case PYTHONRTS:
            fileName = "assignconversionrate.py";
            return modelQualityProxy.uploadPipelineStepRTSPythonScript(fileName, "assigncategorical", requestEntity);
        case PYTHONLEARNING:
            return modelQualityProxy.uploadPipelineStepPythonScript(fileName, "assigncategorical", requestEntity);
        case METADATA:
            return modelQualityProxy.uploadPipelineStepMetadata(fileName, "assigncategorical", requestEntity);
        default:
            return null;
        }

    }

    protected Pipeline createNewDataPipeline(Pipeline sourcePipeline) {
        try {
            uploadPipelineStepFile(PipelineStepType.PYTHONLEARNING);
            uploadPipelineStepFile(PipelineStepType.PYTHONRTS);
            uploadPipelineStepFile(PipelineStepType.METADATA);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<PipelineStepOrFile> pipelineSteps = new ArrayList<>();

        for (PipelineStep step : sourcePipeline.getPipelineSteps()) {
            PipelineStepOrFile p = new PipelineStepOrFile();

            // add the new step type immediately after
            if (step.getMainClassName().equals("EnumeratedColumnTransformStep")) {
                p.pipelineStepName = step.getName();
                pipelineSteps.add(p);
                p = new PipelineStepOrFile();
                Path path = new Path(hdfsDir + "/steps/assigncategorical");
                p.pipelineStepDir = path.toString();
                pipelineSteps.add(p);
                continue;
            } else {
                p.pipelineStepName = step.getName();
            }

            pipelineSteps.add(p);
        }
        Pipeline pipelineAlreadyExists = pipelineEntityMgr.findByName("ModelQualityDeploymentTest-2");
        if (pipelineAlreadyExists != null) {
            System.out.println(String.format("Attempting to delete Pipeline \"%s\"", "ModelQualityDeploymentTest-2"));
            pipelineEntityMgr.delete(pipelineAlreadyExists);
        }

        List<Pipeline> existingPipelines = pipelineEntityMgr.findAll();
        List<Pipeline> pipelinesToDelete = new ArrayList<>();
        for (Pipeline thePipeline : existingPipelines) {
            List<PipelineStep> thePipelineSteps = thePipeline.getPipelineSteps();
            for (PipelineStep theStep : thePipelineSteps) {
                if (theStep.getName().equals("assigncategorical")) {
                    pipelinesToDelete.add(thePipeline);
                    break;
                }
            }
        }
        for (Pipeline thePipeline : pipelinesToDelete) {
            System.out.println(String.format("Attempting to delete Pipeline \"%s\"", thePipeline.getName()));
            pipelineEntityMgr.delete(thePipeline);
        }

        PipelineStep pipelineStepAlreadyExists = pipelineStepEntityMgr.findByName("assigncategorical");
        if (pipelineStepAlreadyExists != null) {
            System.out.println(String.format("Attempting to delete PipelineStep \"%s\"", "assigncategorical"));
            pipelineStepEntityMgr.delete(pipelineStepAlreadyExists);
        }

        String newPipelineName = modelQualityProxy.createPipeline("ModelQualityDeploymentTest-2",
                "ModelQualityDeploymentTest-2", pipelineSteps);
        return pipelineEntityMgr.findByName(newPipelineName);
    }

    protected void waitAndCheckModelRun(String modelName) {

        long start = System.currentTimeMillis();
        while (true) {
            String status = modelQualityProxy.getModelRunStatusByName(modelName);
            ModelRun modelRun = modelRunEntityMgr.findByName(modelName);

            if (status.equals("COMPLETED")) {
                break;
            }

            if (status.equals("FAILED")) {
                Assert.fail("Failed due to= " + modelRun.getErrorMessage());
                break;
            }
            System.out.println(
                    "Waiting for modelRun name \"" + modelName + "\": Status is " + modelRun.getStatus().toString());
            long end = System.currentTimeMillis();
            if ((end - start) > 10 * 3_600_000) { // 10 hours max
                Assert.fail("Timeout for modelRun name \"" + modelName + "\"");
            }
            try {
                Thread.sleep(300_000); // 5 mins
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
