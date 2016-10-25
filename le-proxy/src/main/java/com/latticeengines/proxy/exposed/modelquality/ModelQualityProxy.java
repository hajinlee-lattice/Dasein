package com.latticeengines.proxy.exposed.modelquality;

import java.util.List;

import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.network.exposed.modelquality.ModelQualityAlgorithmInterface;
import com.latticeengines.network.exposed.modelquality.ModelQualityAnalyticPipelineInterface;
import com.latticeengines.network.exposed.modelquality.ModelQualityAnalyticTestInterface;
import com.latticeengines.network.exposed.modelquality.ModelQualityDataFlowInterface;
import com.latticeengines.network.exposed.modelquality.ModelQualityDataSetInterface;
import com.latticeengines.network.exposed.modelquality.ModelQualityModelRunInterface;
import com.latticeengines.network.exposed.modelquality.ModelQualityPipelineInterface;
import com.latticeengines.network.exposed.modelquality.ModelQualityPropDataInterface;
import com.latticeengines.network.exposed.modelquality.ModelQualitySamplingInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("modelQualityProxy")
@SuppressWarnings("unchecked")
public class ModelQualityProxy extends MicroserviceRestApiProxy
        implements ModelQualitySamplingInterface, ModelQualityDataSetInterface, ModelQualityDataFlowInterface,
        ModelQualityAlgorithmInterface, ModelQualityPropDataInterface, ModelQualityModelRunInterface,
        ModelQualityPipelineInterface, ModelQualityAnalyticPipelineInterface, ModelQualityAnalyticTestInterface {

    public ModelQualityProxy() {
        super("modelquality");
    }

    @Override
    public String createModelRun(ModelRunEntityNames modelRunEntityNames, String tenant, String username,
            String password, String apiHostPort) {
        String url = constructUrl(
                "/modelruns/?tenant={tenant}&username={username}&password={password}&apiHostPort={apiHostPort}", //
                tenant, username, password, apiHostPort);
        return post("createModelRun", url, modelRunEntityNames, String.class);
    }

    @Override
    public List<ModelRunEntityNames> getModelRuns() {
        String url = constructUrl("/modelruns/");
        return get("getModelRuns", url, List.class);
    }

    @Override
    public ModelRunEntityNames getModelRunByName(String modelRunName) {
        String url = constructUrl("/modelruns/{modelRunName}", modelRunName);
        return get("getModelRunByName", url, ModelRunEntityNames.class);
    }

    @Override
    public String getModelRunStatusByName(String modelRunName) {
        String url = constructUrl("/modelruns/status/{modelRunName}", modelRunName);
        return get("getModelRunStatusByName", url, String.class);
    }

    @Override
    public String uploadPipelineStepMetadata(String fileName, String stepName, MultipartFile file) {
        String url = constructUrl("/pipelines/pipelinestepfiles/metadata?fileName={fileName}&stepName={stepName}", //
                fileName, stepName);
        return post("uploadPipelineStepMetadata", url, file, String.class);
    }

    @Override
    public String uploadPipelineStepPythonScript(String fileName, String stepName, MultipartFile file) {
        String url = constructUrl("/pipelines/pipelinestepfiles/python?fileName={fileName}&stepName={stepName}", //
                fileName, stepName);
        return post("uploadPipelineStepPythonScript", url, file, String.class);
    }

    @Override
    public String uploadPipelineStepRTSPythonScript(String fileName, String stepName, MultipartFile file) {
        String url = constructUrl("/pipelines/pipelinestepfiles/pythonrts?fileName={fileName}&stepName={stepName}", //
                fileName, stepName);
        return post("uploadPipelineStepRTSPythonScript", url, file, String.class);
    }

    @Override
    public String uploadPipelineStepMetadata(String fileName, //
            String stepName, //
            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity) {
        String url = constructUrl("/pipelines/pipelinestepfiles/metadata?fileName={fileName}&stepName={stepName}", //
                fileName, stepName);
        return post("uploadPipelineStepMetadata", url, requestEntity, String.class);
    }

    @Override
    public String uploadPipelineStepRTSPythonScript(String fileName, //
            String stepName, //
            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity) {
        String url = constructUrl("/pipelines/pipelinestepfiles/pythonrts?fileName={fileName}&stepName={stepName}", //
                fileName, stepName);
        return post("uploadPipelineStepMetadata", url, requestEntity, String.class);
    }

    @Override
    public String uploadPipelineStepPythonScript(String fileName, //
            String stepName, //
            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity) {
        String url = constructUrl("/pipelines/pipelinestepfiles/python?fileName={fileName}&stepName={stepName}", //
                fileName, stepName);
        return post("uploadPipelineStepPythonScript", url, requestEntity, String.class);
    }

    @Override
    public Pipeline createPipelineFromProduction() {
        String url = constructUrl("/pipelines/latest");
        return post("createPipelineFromProduction", url, null, Pipeline.class);
    }

    @Override
    public String createPipeline(String pipelineName, List<PipelineStepOrFile> pipelineSteps) {
        String url = constructUrl("/pipelines/?pipelineName={pipelineName}", pipelineName);
        return post("createPipeline", url, pipelineSteps, String.class);
    }

    @Override
    public List<Pipeline> getPipelines() {
        String url = constructUrl("/pipelines/");
        return get("getPipelines", url, List.class);
    }

    @Override
    public Pipeline getPipelineByName(String pipelineName) {
        String url = constructUrl("/pipelines/{pipelineName}", pipelineName);
        return get("getPipelineByName", url, Pipeline.class);
    }

    @Override
    public List<Sampling> getSamplingConfigs() {
        String url = constructUrl("/samplingconfigs/");
        return get("getSamplings", url, List.class);
    }

    @Override
    public Sampling createSamplingFromProduction() {
        String url = constructUrl("/samplingconfigs/latest");
        return post("createSamplingFromProduction", url, null, Sampling.class);
    }

    @Override
    public String createSamplingConfig(Sampling samplingConfig) {
        String url = constructUrl("/samplingconfigs/");
        return post("createSamplingFromProduction", url, samplingConfig, String.class);
    }

    @Override
    public Sampling getSamplingConfigByName(String samplingConfigName) {
        String url = constructUrl("/samplingconfigs/{samplingConfigName}", samplingConfigName);
        return get("getSamplingConfigByName", url, Sampling.class);
    }

    @Override
    public List<DataSet> getDataSets() {
        String url = constructUrl("/datasets/");
        return get("getDataSets", url, List.class);
    }

    @Override
    public DataSet getDataSetByName(String dataSetName) {
        String url = constructUrl("/datasets/{dataSetName}", dataSetName);
        return get("getDataSetByName", url, DataSet.class);
    }

    @Override
    public String createDataSet(DataSet dataset) {
        String url = constructUrl("/datasets/");
        return post("createDataSet", url, dataset, String.class);
    }

    @Override
    public List<DataFlow> getDataFlows() {
        String url = constructUrl("/dataflows/");
        return get("getDataFlows", url, List.class);
    }

    @Override
    public String createDataFlow(DataFlow dataflow) {
        String url = constructUrl("/dataflows/");
        return post("createDataFlow", url, dataflow, String.class);
    }

    @Override
    public DataFlow getDataFlowByName(String dataFlowName) {
        String url = constructUrl("/dataflows/{dataFlowName}", dataFlowName);
        return get("getDataFlowByName", url, DataFlow.class);
    }

    @Override
    public DataFlow createDataFlowFromProduction() {
        String url = constructUrl("/dataflows/latest");
        return post("createDataFlow", url, null, DataFlow.class);
    }

    @Override
    public List<Algorithm> getAlgorithms() {
        String url = constructUrl("/algorithms/");
        return get("getAlgorithms", url, List.class);
    }

    @Override
    public Algorithm createAlgorithmFromProduction() {
        String url = constructUrl("/algorithms/latest");
        return post("createAlgorithmFromProduction", url, null, Algorithm.class);
    }

    @Override
    public String createAlgorithm(Algorithm algorithm) {
        String url = constructUrl("/algorithms/");
        return post("createAlgorithm", url, algorithm, String.class);
    }

    @Override
    public Algorithm getAlgorithmByName(String algorithmName) {
        String url = constructUrl("/algorithms/{algorithmName}");
        return get("getAlgorithmByName", url, Algorithm.class);
    }

    @Override
    public List<PropData> getPropDataConfigs() {
        String url = constructUrl("/propdataconfigs/");
        return get("getPropDataConfigs", url, List.class);
    }

    @Override
    public String createPropDataConfig(PropData propDataConfig) {
        String url = constructUrl("/propdataconfigs/");
        return post("createPropDataConfig", url, propDataConfig, String.class);
    }

    @Override
    public PropData createPropDataConfigFromProduction() {
        String url = constructUrl("/propdataconfigs/latest");
        return post("createPropDataConfigFromProduction", url, null, PropData.class);
    }

    @Override
    public PropData getPropDataConfigByName(String propDataConfigName) {
        String url = constructUrl("/propdataconfigs/{propDataConfigName}", propDataConfigName);
        return get("getPropDataConfigs", url, PropData.class);
    }

    @Override
    public AnalyticPipelineEntityNames createAnalyticPipelineFromProduction() {
        String url = constructUrl("/analyticpipelines/latest");
        return post("createAnalyticPipelineFromProduction", url, null, AnalyticPipelineEntityNames.class);
    }

    @Override
    public List<AnalyticPipelineEntityNames> getAnalyticPipelines() {
        String url = constructUrl("/analyticpipelines/");
        return get("getAnalyticPipelines", url, List.class);
    }

    @Override
    public String createAnalyticPipeline(AnalyticPipelineEntityNames analyticPipelineEntityNames) {
        String url = constructUrl("/analyticpipelines/");
        return post("createAnalyticPipeline", url, analyticPipelineEntityNames, String.class);
    }

    @Override
    public AnalyticPipelineEntityNames getAnalyticPipelineByName(String analyticPipelineName) {
        String url = constructUrl("/analyticpipelines/{analyticPipelineName}", analyticPipelineName);
        return get("getAnalyticPipelineByName", url, AnalyticPipelineEntityNames.class);
    }

    @Override
    public String createAnalyticTest(AnalyticTestEntityNames analyticTestEntityNames) {
        String url = constructUrl("/analytictests/");
        return post("createAnalyticTest", url, analyticTestEntityNames, String.class);
    }

    @Override
    public List<AnalyticTestEntityNames> getAnalyticTests() {
        String url = constructUrl("/analytictests/");
        return get("getAnalyticTests", url, List.class);
    }

    @Override
    public AnalyticTestEntityNames getAnalyticTestByName(String analyticTestName) {
        String url = constructUrl("/analytictests/{analyticTestName}", analyticTestName);
        return get("getAnalyticTestByName", url, AnalyticTestEntityNames.class);
    }
    
    @Override
    public List<String> executeAnalyticTestByName(String analyticTestName) {
        String url = constructUrl("/analytictests/execute/{analyticTestName}");
        return post("executeAnalyticTestByName", url, analyticTestName, List.class);
    }
}
