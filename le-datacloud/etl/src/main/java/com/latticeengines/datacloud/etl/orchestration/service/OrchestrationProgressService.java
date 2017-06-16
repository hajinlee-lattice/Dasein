package com.latticeengines.datacloud.etl.orchestration.service;

import java.util.List;

import com.latticeengines.datacloud.etl.orchestration.service.impl.OrchestrationProgressServiceImpl;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;

public interface OrchestrationProgressService {
    OrchestrationProgressServiceImpl.OrchestrationProgressUpdater updateProgress(OrchestrationProgress progress);

    List<OrchestrationProgress> createDraftProgresses(Orchestration orch, List<String> triggeredVersions);

    List<OrchestrationProgress> findProgressesToKickoff();

    OrchestrationProgress updateSubmittedProgress(OrchestrationProgress progress, String applicationId);
}
