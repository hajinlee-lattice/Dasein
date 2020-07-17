package com.latticeengines.admin.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

public interface VboRequestLogRepository extends BaseJpaRepository<VboRequestLog, Long> {

    VboRequestLog findByTraceId(String traceId);

    List<VboRequestLog> findByTenantId(String tenantId);
}
