package com.latticeengines.modelquality.functionalframework;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modeling.factory.SamplingFactory;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyDef;
import com.latticeengines.domain.exposed.modelquality.AlgorithmPropertyValue;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.DataSetType;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunStatus;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyDef;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyValue;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyDef;
import com.latticeengines.domain.exposed.modelquality.SamplingPropertyValue;
import com.latticeengines.domain.exposed.modelquality.ScoringDataSet;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.entitymgr.ModelConfigEntityMgr;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;
import com.latticeengines.modelquality.service.ModelRunService;
import com.latticeengines.proxy.exposed.modelquality.ModelQualityProxy;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-modelquality-context.xml" })
public class ModelQualityDeploymentTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    protected AlgorithmEntityMgr algorithmEntityMgr;
    @Autowired
    protected DataFlowEntityMgr dataFlowEntityMgr;
    @Autowired
    protected DataSetEntityMgr dataSetEntityMgr;
    @Autowired
    protected PipelineEntityMgr pipelineEntityMgr;
    @Autowired
    protected PropDataEntityMgr propDataEntityMgr;
    @Autowired
    protected ModelRunEntityMgr modelRunEntityMgr;
    @Autowired
    protected ModelConfigEntityMgr modelConfigEntityMgr;
    @Autowired
    protected SamplingEntityMgr samplingEntityMgr;

    @Autowired
    protected ModelQualityProxy modelQualityProxy;

    @Resource(name = "modelRunService")
    protected ModelRunService modelRunService;

    protected void cleanup() {
        algorithmEntityMgr.deleteAll();
        dataFlowEntityMgr.deleteAll();
        dataSetEntityMgr.deleteAll();
        pipelineEntityMgr.deleteAll();
        propDataEntityMgr.deleteAll();
        samplingEntityMgr.deleteAll();
        modelRunEntityMgr.deleteAll();
        modelConfigEntityMgr.deleteAll();
    }

    protected ModelRun createModelRun(String algoName) {
        SelectedConfig selectedConfig = getSelectedConfig(algoName);
        ModelRun modelRun = new ModelRun();
        modelRun.setName("modelRun1");
        modelRun.setStatus(ModelRunStatus.NEW);
        modelRun.setSelectedConfig(selectedConfig);

        return modelRun;
    }

    protected ModelConfig createModelConfig(String algoName) {
        SelectedConfig selectedConfig = getSelectedConfig(algoName);
        ModelConfig modelConfig = new ModelConfig();
        modelConfig.setName("modelConfig1");
        modelConfig.setSelectedConfig(selectedConfig);
        return modelConfig;
    }

    protected SelectedConfig getSelectedConfig(String algoName) {
        Algorithm algo = createAlgorithm(algoName);
        DataSet dataSet = createDataSet();
        DataFlow dataFlow = createDataFlow();
        PropData propData = createPropData();
        Pipeline pipeline = createPipeline(1);
        Sampling sampling = createSampling();

        SelectedConfig selectedConfig = new SelectedConfig();
        selectedConfig.setAlgorithm(algo);
        selectedConfig.setDataFlow(dataFlow);
        selectedConfig.setDataSet(dataSet);
        selectedConfig.setPipeline(pipeline);
        selectedConfig.setPropData(propData);
        selectedConfig.setSampling(sampling);
        return selectedConfig;
    }

    protected Sampling createSampling() {
        Sampling sampling = new Sampling();
        sampling.setName("sampling1");

        SamplingPropertyDef def = new SamplingPropertyDef();
        def.setName(SamplingFactory.MODEL_SAMPLING_RATE_KEY);
        SamplingPropertyValue value = new SamplingPropertyValue("100");
        def.addSamplingPropertyValue(value);
        sampling.addSamplingPropertyDef(def);

        def.setName(SamplingFactory.MODEL_SAMPLING_TRAINING_PERCENTAGE_KEY);
        value = new SamplingPropertyValue("80");
        def.addSamplingPropertyValue(value);
        sampling.addSamplingPropertyDef(def);

        def.setName(SamplingFactory.MODEL_SAMPLING_TEST_PERCENTAGE_KEY);
        value = new SamplingPropertyValue("20");
        def.addSamplingPropertyValue(value);
        sampling.addSamplingPropertyDef(def);

        return sampling;
    }

    protected DataFlow createDataFlow() {
        DataFlow dataFlow = new DataFlow();
        dataFlow.setName("dataFlow1");
        dataFlow.setMatch(true);
        dataFlow.setTransformationGroup(TransformationGroup.STANDARD);
        dataFlow.setDedupType(DedupType.ONELEADPERDOMAIN);
        return dataFlow;
    }

    protected Pipeline createPipeline(int i) {

        Pipeline pipeline = new Pipeline();
        pipeline.setName("Pipeline" + i);
        pipeline.setPipelineScript("/app/dataplatform/scripts/pipeline/pipeline.py");
        pipeline.setPipelineLibScript("/app/dataplatform/scripts/lepipeline.tar.gz");
        pipeline.setPipelineDriver("/app/dataplatform/scripts/pipeline/pipeline.json");

        PipelineStep step = new PipelineStep();
        step.setName("pivotstep");
        step.setScript("pivotstep.py");
        List<PipelinePropertyDef> pipelinePropertyDefs = new ArrayList<>();
        PipelinePropertyDef pipelinePropertyDef = new PipelinePropertyDef();
        pipelinePropertyDef.setName("minCategoricalCount");
        PipelinePropertyValue pipeLinePropertyValue = new PipelinePropertyValue();
        pipeLinePropertyValue.setValue("5");
        pipelinePropertyDef.addPipelinePropertyValue(pipeLinePropertyValue);
        pipelinePropertyDefs.add(pipelinePropertyDef);
        step.addPipelinePropertyDef(pipelinePropertyDef);
        pipeline.addPipelineStep(step);
        
        step = new PipelineStep();
        step.setName("assignconversionratetoallcategoricalvalues");
        step.setScript("assignconversionratetoallcategoricalvalues.py");
        pipelinePropertyDefs = new ArrayList<>();
        pipelinePropertyDef = new PipelinePropertyDef();
        pipelinePropertyDef.setName("enabled");
        pipeLinePropertyValue = new PipelinePropertyValue();
        pipeLinePropertyValue.setValue("true");
        pipelinePropertyDef.addPipelinePropertyValue(pipeLinePropertyValue);
        pipelinePropertyDefs.add(pipelinePropertyDef);
        step.addPipelinePropertyDef(pipelinePropertyDef);
//        pipeline.addPipelineStep(step);

        return pipeline;
    }

    protected PropData createPropData() {
        PropData propData = new PropData();
        propData.setName("propData1");
        propData.setMetadataVersion("metadataVersion1");
        propData.setVersion("");
        return propData;
    }

    protected DataSet createDataSet() {

        DataSet dataSet = new DataSet();
        dataSet.setName("DataSet1"); 
        dataSet.setIndustry("Industry1");
        dataSet.setTenant(new Tenant("Model_Quality_Test.Model_Quality_Test.Production"));
        dataSet.setDataSetType(DataSetType.SOURCETABLE);
        dataSet.setSchemaInterpretation(SchemaInterpretation.SalesforceLead);
        dataSet.setTrainingSetHdfsPath("/Pods/Default/Services/ModelQuality/Mulesoft_MKTO_LP3_ScoringLead_20160316_170113.csv");
        ScoringDataSet scoringDataSet = new ScoringDataSet();
        scoringDataSet.setName("ScoringDataSet1");
        scoringDataSet.setDataHdfsPath("ScoringDataSetPath1");
        dataSet.addScoringDataSet(scoringDataSet);
        return dataSet;
    }

    protected Algorithm createAlgorithm(String algo) {
        if (algo.equals(AlgorithmFactory.ALGORITHM_NAME_RF)) {
            return getRF();
        } else if (algo.equals(AlgorithmFactory.ALGORITHM_NAME_LR)) {
            return getLR();
        }
        return null;
    }

    private Algorithm getLR() {
        Algorithm algorithm = new Algorithm();
        algorithm.setName(AlgorithmFactory.ALGORITHM_NAME_LR);
        AlgorithmPropertyDef def = new AlgorithmPropertyDef("C");
        AlgorithmPropertyValue value = new AlgorithmPropertyValue("1.0");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        return algorithm;
    }

    private Algorithm getRF() {

        Algorithm algorithm = new Algorithm();
        algorithm.setName(AlgorithmFactory.ALGORITHM_NAME_RF);
        algorithm.setScript("/app/dataplatform/scripts/algorithm/rf_train.py");

        AlgorithmPropertyDef def = new AlgorithmPropertyDef("n_estimators");
        AlgorithmPropertyValue value = new AlgorithmPropertyValue("100");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("criterion");
        value = new AlgorithmPropertyValue("gini");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("n_jobs");
        value = new AlgorithmPropertyValue("5");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("min_samples_split");
        value = new AlgorithmPropertyValue("25");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("min_samples_leaf");
        value = new AlgorithmPropertyValue("10");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("max_depth");
        value = new AlgorithmPropertyValue("8");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("bootstrap");
        value = new AlgorithmPropertyValue("True");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("calibration_width");
        value = new AlgorithmPropertyValue("4");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        def = new AlgorithmPropertyDef("cross_validation");
        value = new AlgorithmPropertyValue("5");
        def.addAlgorithmPropertyValue(value);
        algorithm.addAlgorithmPropertyDef(def);

        
        return algorithm;

    }

}
