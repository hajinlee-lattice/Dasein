package com.latticeengines.datacloudapi.engine.orchestration.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;

public interface OrchestrationService {
    List<OrchestrationProgress> scan(String hdfsPod);

    DataCloudEngineStage getDataCloudEngineStatus(DataCloudEngineStage stage);
}
