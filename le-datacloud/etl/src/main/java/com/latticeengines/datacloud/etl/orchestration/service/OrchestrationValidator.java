package com.latticeengines.datacloud.etl.orchestration.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;

public interface OrchestrationValidator {
    boolean isTriggered(Orchestration orch, List<String> triggeredVersions);
}
