package com.latticeengines.datacloud.etl.service;

import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.EngineProgress;

public interface DataCloudEngineService {
    DataCloudEngine getEngine();

    String findCurrentVersion(String engineName);

    EngineProgress findProgressAtVersion(String engineName, String version);
}
