package com.latticeengines.admin.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

public interface VboRequestLogEntityMgr extends BaseEntityMgrRepository<VboRequestLog, Long> {

    VboRequestLog findByTraceId(String traceId);

    VboRequestLog findByTenantId(String tenantId);

    void save(VboRequestLog vboRequestLog);
}
