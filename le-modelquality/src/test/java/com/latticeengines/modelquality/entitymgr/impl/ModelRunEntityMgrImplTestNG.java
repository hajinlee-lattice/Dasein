package com.latticeengines.modelquality.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.DataSetType;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunStatus;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyDef;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyValue;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.AlgorithmService;
import com.latticeengines.modelquality.service.AnalyticPipelineService;
import com.latticeengines.modelquality.service.DataFlowService;
import com.latticeengines.modelquality.service.PipelineService;
import com.latticeengines.modelquality.service.PropDataService;
import com.latticeengines.modelquality.service.SamplingService;

public class ModelRunEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private AlgorithmEntityMgr algorithmEntityMgr;

    @Autowired
    private AlgorithmService algorithmService;

    @Autowired
    private AnalyticPipelineEntityMgr analyticPipelineEntityMgr;

    @Autowired
    private DataFlowEntityMgr dataFlowEntityMgr;

    @Autowired
    private DataFlowService dataFlowService;

    @Autowired
    private DataSetEntityMgr dataSetEntityMgr;

    @Autowired
    private PipelineEntityMgr pipelineEntityMgr;

    @Autowired
    private PipelineService pipelineService;

    @Autowired
    private PropDataEntityMgr propDataEntityMgr;

    @Autowired
    private PropDataService propDataService;

    @Autowired
    private SamplingEntityMgr samplingEntityMgr;

    @Autowired
    private SamplingService samplingService;

    @Autowired
    private ModelRunEntityMgr modelRunEntityMgr;

    @Autowired
    private AnalyticPipelineService analyticPipelineService;

    private ModelRun modelRun;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        algorithmEntityMgr.deleteAll();
        analyticPipelineEntityMgr.deleteAll();
        dataFlowEntityMgr.deleteAll();
        dataSetEntityMgr.deleteAll();
        pipelineEntityMgr.deleteAll();
        propDataEntityMgr.deleteAll();
        samplingEntityMgr.deleteAll();
        modelRunEntityMgr.deleteAll();

        Algorithm algorithm = algorithmService.createLatestProductionAlgorithm();
        DataFlow dataflow = dataFlowService.createLatestProductionDataFlow();

        DataSet dataset = new DataSet();
        dataset.setName("ModelRunEntityMgrImplTestNGDataSet");
        Tenant tenant = new Tenant("CustomerSpace");
        dataset.setTenant(tenant);
        dataset.setIndustry("An Industry");
        dataset.setDataSetType(DataSetType.FILE);
        dataset.setSchemaInterpretation(SchemaInterpretation.SalesforceLead);
        dataSetEntityMgr.create(dataset);
        Pipeline pipeline = createLocalPipeline();
        PropData propdata = propDataService.createLatestProductionPropData();
        Sampling sampling = samplingService.createLatestProductionSamplingConfig();

        AnalyticPipelineEntityNames analyticPipelineEntityNames = new AnalyticPipelineEntityNames();
        analyticPipelineEntityNames.setName("analyticPipeline1");
        analyticPipelineEntityNames.setPipeline(pipeline.getName());
        analyticPipelineEntityNames.setAlgorithm(algorithm.getName());
        analyticPipelineEntityNames.setPropData(propdata.getName());
        analyticPipelineEntityNames.setDataFlow(dataflow.getName());
        analyticPipelineEntityNames.setSampling(sampling.getName());

        AnalyticPipeline analyticPipeline = analyticPipelineService.createAnalyticPipeline(analyticPipelineEntityNames);

        modelRun = new ModelRun();
        modelRun.setName("modelRun1");
        modelRun.setDescription("Test pipeline for persistence.");
        modelRun.setCreated(new Date());
        modelRun.setStatus(ModelRunStatus.NEW);
        modelRun.setAnalyticPipeline(analyticPipeline);
        modelRun.setDataSet(dataset);
    }

    private Pipeline createLocalPipeline() {
        Pipeline pipeline = new Pipeline();
        pipeline.setName("Test pipeline");
        PipelineStep pipelineStep = new PipelineStep();
        pipelineStep.setName("pivotstep");
        pipelineStep.setScript("pivotstep.py");
        PipelinePropertyDef pipelinePropertyDef = new PipelinePropertyDef("pivotstep.threshold");
        PipelinePropertyValue pipelinePropertyValue = new PipelinePropertyValue("0.10");

        pipeline.addPipelineStep(pipelineStep);
        pipelineStep.addPipelinePropertyDef(pipelinePropertyDef);
        pipelinePropertyDef.addPipelinePropertyValue(pipelinePropertyValue);

        return pipeline;
    }

    @Test(groups = "functional")
    public void create() throws Exception {
        modelRunEntityMgr.create(modelRun);

        List<ModelRun> retrievedModelRuns = modelRunEntityMgr.findAll();
        assertEquals(retrievedModelRuns.size(), 1);
        ModelRun retrievedModelRun = retrievedModelRuns.get(0);
        assertEquals(retrievedModelRun.getName(), modelRun.getName());
        assertEquals(retrievedModelRun.getDescription(), modelRun.getDescription());
        assertEquals(retrievedModelRun.getStatus(), modelRun.getStatus());

        SelectedConfig selectedConfig = new SelectedConfig();
        AnalyticPipeline analyticPipeline = retrievedModelRun.getAnalyticPipeline();
        DataSet dataset = retrievedModelRun.getDataSet();
        selectedConfig.setPipeline(analyticPipeline.getPipeline());
        selectedConfig.setAlgorithm(analyticPipeline.getAlgorithm());
        selectedConfig.setDataSet(dataset);
        selectedConfig.setPropData(analyticPipeline.getPropData());
        selectedConfig.setDataFlow(analyticPipeline.getDataFlow());
        selectedConfig.setSampling(analyticPipeline.getSampling());
        System.out.println(JsonUtils.serialize(selectedConfig));
    }
}
