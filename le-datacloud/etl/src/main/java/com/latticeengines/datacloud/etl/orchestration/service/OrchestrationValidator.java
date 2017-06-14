package com.latticeengines.datacloud.etl.orchestration.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;

public interface OrchestrationValidator {
    boolean isTriggered(Orchestration orch, List<String> triggeredVersions);

    List<OrchestrationProgress> cleanupDuplicateProgresses(List<OrchestrationProgress> progresses);
}
