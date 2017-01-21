package com.latticeengines.modelquality.service.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class ManyServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testServiceImpl() throws Exception {

        String analyticTestStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/analytictest.json")
                        .getFile()));
        AnalyticTestEntityNames analyticTestEntityNames = JsonUtils.deserialize(analyticTestStr,
                AnalyticTestEntityNames.class);

        AnalyticTest analyticTestAlreadyExists = analyticTestEntityMgr.findByName(analyticTestEntityNames.getName());
        if (analyticTestAlreadyExists != null)
            analyticTestEntityMgr.delete(analyticTestAlreadyExists);

        String analyticPipelineStr = FileUtils.readFileToString(new File( //
                ClassLoader
                        .getSystemResource("com/latticeengines/modelquality/functionalframework/analyticpipeline.json")
                        .getFile()));
        AnalyticPipelineEntityNames analyticPipelineEntityNames = JsonUtils.deserialize(analyticPipelineStr,
                AnalyticPipelineEntityNames.class);
        AnalyticPipeline analyticPipelineAlreadyExists = analyticPipelineEntityMgr
                .findByName(analyticPipelineEntityNames.getName());
        if (analyticPipelineAlreadyExists != null)
            analyticPipelineEntityMgr.delete(analyticPipelineAlreadyExists);

        String algorithmStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/algorithm.json")
                        .getFile()));
        Algorithm algorithm = JsonUtils.deserialize(algorithmStr, Algorithm.class);
        Algorithm algorithmAlreadyExists = algorithmEntityMgr.findByName(algorithm.getName());
        if (algorithmAlreadyExists == null) {
            algorithmEntityMgr.create(algorithm);
        }

        String dataflowStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/dataflow.json")
                        .getFile()));
        DataFlow dataflow = JsonUtils.deserialize(dataflowStr, DataFlow.class);
        DataFlow dataflowAlreadyExists = dataFlowEntityMgr.findByName(dataflow.getName());
        if (dataflowAlreadyExists != null)
            dataFlowEntityMgr.delete(dataflowAlreadyExists);
        dataFlowEntityMgr.create(dataflow);

        String propDataStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/propdata.json")
                        .getFile()));
        PropData propData = JsonUtils.deserialize(propDataStr, PropData.class);
        PropData propDataAlreadyExists = propDataEntityMgr.findByName(propData.getName());
        if (propDataAlreadyExists != null)
            propDataEntityMgr.delete(propDataAlreadyExists);
        propDataEntityMgr.create(propData);

        String samplingStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/sampling.json")
                        .getFile()));
        Sampling sampling = JsonUtils.deserialize(samplingStr, Sampling.class);
        Sampling samplingAlreadyExists = samplingEntityMgr.findByName(sampling.getName());
        if (samplingAlreadyExists != null)
            samplingEntityMgr.delete(samplingAlreadyExists);
        samplingEntityMgr.create(sampling);

        PipelineStep pipelineStepAlreadyExists = pipelineStepEntityMgr.findByName("remediatedatarulesstep");
        if (pipelineStepAlreadyExists == null) {
            pipelineService.createLatestProductionPipeline();
        }

        String pipelineStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/pipeline.json")
                        .getFile()));
        Pipeline pipeline = JsonUtils.deserialize(pipelineStr, Pipeline.class);
        Pipeline pipelineAlreadyExists = pipelineEntityMgr.findByName(pipeline.getName());
        if (pipelineAlreadyExists != null)
            pipelineEntityMgr.delete(pipelineAlreadyExists);

        List<PipelineStep> pipelineSteps = pipeline.getPipelineSteps();
        List<PipelineStepOrFile> pipelineStepsOrFiles = new ArrayList<>();
        for (PipelineStep p : pipelineSteps) {
            PipelineStepOrFile psof = new PipelineStepOrFile();
            psof.pipelineStepName = p.getName();
            pipelineStepsOrFiles.add(psof);
        }

        pipeline = pipelineService.createPipeline(pipeline.getName(), pipeline.getDescription(), pipelineStepsOrFiles);

        String datasetStr = FileUtils.readFileToString(new File( //
                ClassLoader.getSystemResource("com/latticeengines/modelquality/functionalframework/dataset.json")
                        .getFile()));
        DataSet dataset = JsonUtils.deserialize(datasetStr, DataSet.class);
        DataSet datasetAlreadyExists = dataSetEntityMgr.findByName(dataset.getName());
        if (datasetAlreadyExists != null)
            dataSetEntityMgr.delete(datasetAlreadyExists);
        dataSetEntityMgr.create(dataset);

        AnalyticPipeline analyticPipeline = analyticPipelineService.createAnalyticPipeline(analyticPipelineEntityNames);

        List<String> analyticPipelineNames = new ArrayList<>();
        analyticPipelineNames.add(analyticPipeline.getName());
        List<String> datasetNames = new ArrayList<>();
        datasetNames.add(dataset.getName());
        analyticTestEntityNames.setAnalyticPipelineNames(analyticPipelineNames);
        analyticTestEntityNames.setDataSetNames(datasetNames);
        AnalyticTest analyticTest = analyticTestService.createAnalyticTest(analyticTestEntityNames);

        analyticTestEntityMgr.delete(analyticTest);
        analyticPipelineEntityMgr.delete(analyticPipeline);
        dataSetEntityMgr.delete(dataset);
        pipelineEntityMgr.delete(pipeline);
        samplingEntityMgr.delete(sampling);
        propDataEntityMgr.delete(propData);
        dataFlowEntityMgr.delete(dataflow);
    }

}
