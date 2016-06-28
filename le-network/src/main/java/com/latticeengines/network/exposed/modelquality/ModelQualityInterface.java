package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.Algorithm;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.Sampling;

public interface ModelQualityInterface {

    ResponseDocument<String> runModel(ModelRun modelRun);

    ResponseDocument<List<ModelRun>> getModelRuns();

    void deleteModelRuns();

    ResponseDocument<List<Algorithm>> getAlgorithms();

    ResponseDocument<String> upsertAlgorithms(List<Algorithm> algorithms);

    ResponseDocument<List<DataFlow>> getDataFlows();

    ResponseDocument<String> upsertDataFlows(List<DataFlow> dataflows);

    ResponseDocument<List<DataSet>> getDataSets();

    ResponseDocument<String> upsertDataSets(List<DataSet> datasets);

    ResponseDocument<List<Pipeline>> getPipelines();

    ResponseDocument<String> upsertPipelines(List<Pipeline> pipelines);

    ResponseDocument<List<PropData>> getPropDatas();

    ResponseDocument<String> upsertPropDatas(List<PropData> propdatas);

    ResponseDocument<List<Sampling>> getSamplings();

    ResponseDocument<String> upsertSamplings(List<Sampling> samplings);

    ResponseDocument<String> upsertModelConfigs(List<ModelConfig> modelConfigs);

    ResponseDocument<List<ModelConfig>> getModelConfigs();

}
