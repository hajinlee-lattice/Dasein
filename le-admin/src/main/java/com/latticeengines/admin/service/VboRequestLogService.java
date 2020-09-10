package com.latticeengines.admin.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.vbo.VboCallback;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

public interface VboRequestLogService {

    void createVboRequestLog(String traceId, String tenantId, Long receiveTime, VboRequest vboRequest, VboResponse vboResponse);

    void updateVboResponse(String traceId, VboResponse vboResponse);

    void updateVboCallback(String traceId, VboCallback vboCallback, Long sendTime);

    VboRequestLog getVboRequestLogByTraceId(String traceId);

    List<VboRequestLog> getVboRequestLogByTenantId(String tenantId);
}
