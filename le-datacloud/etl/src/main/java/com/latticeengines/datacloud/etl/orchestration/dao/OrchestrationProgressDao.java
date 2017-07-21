package com.latticeengines.datacloud.etl.orchestration.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;

public interface OrchestrationProgressDao extends BaseDao<OrchestrationProgress> {
    List<OrchestrationProgress> findProgressesByField(Map<String, Object> fields, List<String> orderFields);

    List<OrchestrationProgress> findProgressesToCheckStatus();

    boolean isDuplicateVersion(String orchName, String version);
}
