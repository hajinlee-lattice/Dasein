package com.latticeengines.datacloud.etl.service;

import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;

public interface DataCloudEngineService {
    DataCloudEngine getEngine();

    String findCurrentVersion(String engineName);
}
