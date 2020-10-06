package com.latticeengines.admin.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.vbo.VboCallback;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

public interface VboRequestLogEntityMgr extends BaseEntityMgrRepository<VboRequestLog, Long> {

    VboRequestLog findByTraceId(String traceId);

    List<VboRequestLog> findByTenantId(String tenantId);

    VboRequestLog save(VboRequestLog vboRequestLog);

    void updateVboResponse(String traceId, VboResponse vboResponse);

    void updateVboCallback(String traceId, VboCallback vboCallback, Long sendTime);
}
