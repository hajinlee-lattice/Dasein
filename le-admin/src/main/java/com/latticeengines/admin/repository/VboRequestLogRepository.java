package com.latticeengines.admin.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

public interface VboRequestLogRepository extends BaseJpaRepository<VboRequestLog, Long> {

    VboRequestLog findByTraceId(String traceId);

    VboRequestLog findByTenantId(String tenantId);
}
