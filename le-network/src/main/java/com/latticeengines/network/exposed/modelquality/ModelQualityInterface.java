package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.domain.exposed.modelquality.ModelRun;

public interface ModelQualityInterface extends ModelQualityPipelineInterface {

    ResponseDocument<String> runModel(ModelRun modelRun, //
            String tenant, String username, String password, String apiHostPort);

    ResponseDocument<List<ModelRun>> getModelRuns();

    void deleteModelRuns();

    ResponseDocument<String> upsertModelConfigs(List<ModelConfig> modelConfigs);

    ResponseDocument<List<ModelConfig>> getModelConfigs();
    
    ResponseDocument<ModelRun> getModelRun(String modelRunId);

}
