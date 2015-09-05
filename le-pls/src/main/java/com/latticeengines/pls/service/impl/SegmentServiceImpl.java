package com.latticeengines.pls.service.impl;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.SegmentEntityMgr;
import com.latticeengines.pls.service.SegmentService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;

@Component("segmentService")
public class SegmentServiceImpl implements SegmentService {

    @Autowired
    private SessionService sessionService;

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    

    @Override
    public void createSegment(Segment segment, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        segment.setTenant(tenant);
        segmentEntityMgr.create(segment);
    }


    @Override
    public void update(String segmentName, Segment segment) {
        Segment segmentFromDb = segmentEntityMgr.findByName(segmentName);
        
        if (segmentFromDb == null) {
            throw new LedpException(LedpCode.LEDP_18025, new String[] { segmentName });
        }

        if (!segmentName.equals(segment.getName())) {
            throw new LedpException(LedpCode.LEDP_18026);
        }
        
        segmentFromDb.setModelId(segment.getModelId());
        segmentFromDb.setPriority(segment.getPriority());
        segmentEntityMgr.update(segmentFromDb);
    }


}
