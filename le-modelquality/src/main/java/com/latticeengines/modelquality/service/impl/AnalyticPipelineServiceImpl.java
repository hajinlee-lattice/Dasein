package com.latticeengines.modelquality.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;
import com.latticeengines.modelquality.service.AnalyticPipelineService;
import com.latticeengines.proxy.exposed.modelquality.ModelQualityProxy;

@Component("analyticPipelineService")
public class AnalyticPipelineServiceImpl extends BaseServiceImpl implements AnalyticPipelineService {

    private static final Log log = LogFactory.getLog(AnalyticPipelineServiceImpl.class);

    @Autowired
    private AnalyticPipelineEntityMgr analyticPipelineEntityMgr;

    @Autowired
    private PipelineEntityMgr pipelineEntityMgr;

    @Autowired
    private AlgorithmEntityMgr algorithmEntityMgr;

    @Autowired
    private PropDataEntityMgr propdataEntityMgr;

    @Autowired
    private DataFlowEntityMgr dataflowEntityMgr;

    @Autowired
    private SamplingEntityMgr samplingEntityMgr;

    @Autowired
    private ModelQualityProxy modelQualityProxy;

    private final String production = "Production";

    @Override
    public AnalyticPipeline createAnalyticPipeline(AnalyticPipelineEntityNames analyticPipelineEntityNames) {
        AnalyticPipeline analyticPipeline = new AnalyticPipeline();

        if (analyticPipelineEntityNames.getName() == null || analyticPipelineEntityNames.getName().trim().isEmpty()) {
            throw new RuntimeException("AnalyticPipeline Name cannot be empty");
        }
        analyticPipeline.setName(analyticPipelineEntityNames.getName());

        Pipeline pipeline = pipelineEntityMgr.findByName(analyticPipelineEntityNames.getPipeline());
        if (pipeline == null) {
            throw new RuntimeException(
                    String.format("Pipeline with name %s does not exist", analyticPipelineEntityNames.getPipeline()));
        }
        analyticPipeline.setPipeline(pipeline);

        Algorithm algorithm = algorithmEntityMgr.findByName(analyticPipelineEntityNames.getAlgorithm());
        if (algorithm == null) {
            throw new RuntimeException(
                    String.format("Algorithm with name %s does not exist", analyticPipelineEntityNames.getAlgorithm()));
        }
        analyticPipeline.setAlgorithm(algorithm);

        PropData propdata = propdataEntityMgr.findByName(analyticPipelineEntityNames.getPropData());
        if (propdata == null) {
            throw new RuntimeException(String.format("A Propdata match with name %s does not exist",
                    analyticPipelineEntityNames.getPropData()));
        }
        analyticPipeline.setPropData(propdata);

        DataFlow dataFlow = dataflowEntityMgr.findByName(analyticPipelineEntityNames.getDataFlow());
        if (dataFlow == null) {
            throw new RuntimeException(
                    String.format("Dataflow with name %s does not exist", analyticPipelineEntityNames.getDataFlow()));
        }
        analyticPipeline.setDataFlow(dataFlow);

        Sampling sampling = samplingEntityMgr.findByName(analyticPipelineEntityNames.getSampling());
        if (sampling == null) {
            throw new RuntimeException(
                    String.format("Sampling type named %s does not exist", analyticPipelineEntityNames.getSampling()));
        }
        analyticPipeline.setSampling(sampling);

        analyticPipelineEntityMgr.create(analyticPipeline);

        return analyticPipeline;
    }

    @Override
    public AnalyticPipeline createLatestProductionAnalyticPipeline() {
        String version = getVersion();

        AnalyticPipelineEntityNames analyticPipelineEntityNames = new AnalyticPipelineEntityNames();
        analyticPipelineEntityNames.setName(production + "-" + version);
        analyticPipelineEntityNames.setAlgorithm(modelQualityProxy.createAlgorithmFromProduction().getName());
        analyticPipelineEntityNames.setDataFlow(modelQualityProxy.createDataFlowFromProduction().getName());
        analyticPipelineEntityNames.setPipeline(modelQualityProxy.createPipelineFromProduction().getName());
        analyticPipelineEntityNames.setPropData(modelQualityProxy.createPropDataConfigFromProduction().getName());
        analyticPipelineEntityNames.setSampling(modelQualityProxy.createSamplingFromProduction().getName());

        AnalyticPipeline analyticPipeline = createAnalyticPipeline(analyticPipelineEntityNames);

        return analyticPipeline;
    }

}
