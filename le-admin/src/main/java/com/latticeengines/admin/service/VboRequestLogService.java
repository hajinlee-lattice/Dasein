package com.latticeengines.admin.service;

import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

public interface VboRequestLogService {

    void createVboRequestLog(String traceId, String tenantId, VboRequest vboRequest, VboResponse vboResponse);

    VboRequestLog getVboRequestLogByTraceId(String traceId);

    VboRequestLog getVboRequestLogByTenantId(String tenantId);
}
