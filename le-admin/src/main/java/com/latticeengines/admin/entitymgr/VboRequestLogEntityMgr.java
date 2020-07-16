package com.latticeengines.admin.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

public interface VboRequestLogEntityMgr extends BaseEntityMgrRepository<VboRequestLog, Long> {

    VboRequestLog findByTraceId(String traceId);

    List<VboRequestLog> findByTenantId(String tenantId);

    void save(VboRequestLog vboRequestLog);
}
