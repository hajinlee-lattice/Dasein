package com.latticeengines.datacloud.etl.orchestration.entitymgr;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;

public interface OrchestrationProgressEntityMgr extends BaseEntityMgr<OrchestrationProgress> {
    List<OrchestrationProgress> findProgressesByField(Map<String, Object> fields, List<String> orderFields);

    OrchestrationProgress findProgress(OrchestrationProgress progress);

    OrchestrationProgress findProgress(Long pid);

    OrchestrationProgress saveProgress(OrchestrationProgress progress);

    void saveProgresses(List<OrchestrationProgress> progresses);

    List<OrchestrationProgress> findProgressesToCheckStatus();

    boolean isDuplicateVersion(String orchName, String version);

    boolean hasJobInProgress(String orchName);

    boolean hasTriggeredSince(String orchName, Date since);
}
