package com.latticeengines.admin.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.admin.entitymgr.VboRequestLogEntityMgr;
import com.latticeengines.admin.service.VboRequestLogService;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

@Service("vboRequestLogService")
public class VboRequestLogServiceImpl implements VboRequestLogService {
    
    @Inject
    private VboRequestLogEntityMgr vboRequestLogEntityMgr;

    @Override
    public void createVboRequestLog(String traceId, String tenantId, VboRequest vboRequest, VboResponse vboResponse) {
        VboRequestLog vboRequestLog = new VboRequestLog();
        vboRequestLog.setTraceId(traceId);
        vboRequestLog.setTenantId(tenantId);
        vboRequestLog.setVboRequest(vboRequest);
        vboRequestLog.setVboResponse(vboResponse);
        vboRequestLogEntityMgr.save(vboRequestLog);
    }

    @Override
    public void updateVboResponse(String traceId, VboResponse vboResponse) {
        vboRequestLogEntityMgr.updateVboResponse(traceId, vboResponse);
    }

    @Override
    public VboRequestLog getVboRequestLogByTraceId(String traceId) {
        return vboRequestLogEntityMgr.findByTraceId(traceId);
    }

    @Override
    public List<VboRequestLog> getVboRequestLogByTenantId(String tenantId) {
        return vboRequestLogEntityMgr.findByTenantId(tenantId);
    }
}
