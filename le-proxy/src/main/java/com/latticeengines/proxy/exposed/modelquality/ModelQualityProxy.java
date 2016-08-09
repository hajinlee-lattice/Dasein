package com.latticeengines.proxy.exposed.modelquality;

import java.util.List;

import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.network.exposed.modelquality.ModelQualityInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("modelQualityProxy")
@SuppressWarnings("unchecked")
public class ModelQualityProxy extends MicroserviceRestApiProxy implements ModelQualityInterface {

    public ModelQualityProxy() {
        super("modelquality");
    }

    @Override
    public ResponseDocument<String> runModel(ModelRun modelRun, String tenant, String username,
            String encryptedPassword, String apiHostPort) {
        String url = constructUrl(
                "/runmodel?tenant={tenant}&username={username}&password={password}&apiHostPort={apiHostPort}", //
                tenant, username, encryptedPassword, apiHostPort);
        return post("runModel", url, modelRun, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<List<ModelRun>> getModelRuns() {
        String url = constructUrl("/modelruns");
        return get("getModelRuns", url, ResponseDocument.class);
    }

    @Override
    public void deleteModelRuns() {
        String url = constructUrl("/modelruns");
        delete("deleteModelRuns", url);
    }

    @Override
    public ResponseDocument<List<Algorithm>> getAlgorithms() {
        String url = constructUrl("/algorithms");
        return get("getAlgorithms", url, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<String> upsertAlgorithms(List<Algorithm> algorithms) {
        String url = constructUrl("/algorithms");
        return post("upsertAlgorithms", url, algorithms, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<List<DataFlow>> getDataFlows() {
        String url = constructUrl("/dataflows");
        return get("getDataFlows", url, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<String> upsertDataFlows(List<DataFlow> dataflows) {
        String url = constructUrl("/dataflows");
        return post("upsertDataFlows", url, dataflows, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<List<DataSet>> getDataSets() {
        String url = constructUrl("/datasets");
        return get("getDataSets", url, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<String> upsertDataSets(List<DataSet> datasets) {
        String url = constructUrl("/datasets");
        return post("upsertDataSets", url, datasets, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<List<Pipeline>> getPipelines() {
        String url = constructUrl("/pipelines");
        return get("getPipelines", url, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<String> upsertPipelines(List<Pipeline> pipelines) {
        String url = constructUrl("/pipelines");
        return post("upsertPipelines", url, pipelines, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<List<PropData>> getPropDatas() {
        String url = constructUrl("/propdatas");
        return get("getPropDatas", url, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<String> upsertPropDatas(List<PropData> propdatas) {
        String url = constructUrl("/propdatas");
        return post("upsertPropDatas", url, propdatas, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<List<Sampling>> getSamplings() {
        String url = constructUrl("/samplings");
        return get("getSamplings", url, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<String> upsertSamplings(List<Sampling> samplings) {
        String url = constructUrl("/samplings");
        return post("upsertSamplings", url, samplings, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<List<ModelConfig>> getModelConfigs() {
        String url = constructUrl("/modelconfigs");
        return get("getModelConfigs", url, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<String> upsertModelConfigs(List<ModelConfig> modelConfigs) {
        String url = constructUrl("/modelconfigs");
        return post("upsertModelConfig", url, modelConfigs, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<String> uploadPipelineStepFile(String fileName,
            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity) {
        String url = constructUrl("/pipelinestepfile?fileName={fileName}", fileName);
        return post("uploadPipelineStepFile", url, requestEntity, ResponseDocument.class);
    }

    @Override
    public ResponseDocument<ModelRun> getModelRun(String modelRunId) {
        String url = constructUrl("/modelrun/{modelRunId}", modelRunId);
        return get("getModelRun", url, ResponseDocument.class);
    }

}
