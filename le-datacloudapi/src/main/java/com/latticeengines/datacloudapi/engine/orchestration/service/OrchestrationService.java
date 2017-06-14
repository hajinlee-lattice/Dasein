package com.latticeengines.datacloudapi.engine.orchestration.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;

public interface OrchestrationService {
    List<OrchestrationProgress> scan(String hdfsPod);
}
