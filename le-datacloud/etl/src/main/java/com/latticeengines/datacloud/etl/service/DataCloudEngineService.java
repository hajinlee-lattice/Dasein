package com.latticeengines.datacloud.etl.service;

import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;

public interface DataCloudEngineService {
    DataCloudEngine getEngine();

    String findCurrentVersion(String engineName);

    DataCloudEngineStage findProgressAtVersion(DataCloudEngineStage job);
}
