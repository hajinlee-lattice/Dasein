package com.latticeengines.datacloud.etl.orchestration.entitymgr;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;

public interface OrchestrationProgressEntityMgr {
    List<OrchestrationProgress> findProgressesByField(Map<String, Object> fields, List<String> orderFields);

    OrchestrationProgress findProgress(OrchestrationProgress progress);

    OrchestrationProgress saveProgress(OrchestrationProgress progress);

    void saveProgresses(List<OrchestrationProgress> progresses);

    List<OrchestrationProgress> findProgressesToCheckStatus();

    boolean isDuplicateVersion(String orchName, String version);
}
