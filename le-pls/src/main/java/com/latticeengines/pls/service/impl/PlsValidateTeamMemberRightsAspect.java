package com.latticeengines.pls.service.impl;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.User;

@Aspect
public class PlsValidateTeamMemberRightsAspect {

    @Before("execution(public * com.latticeengines.pls.service.impl.MetadataSegmentServiceImpl.create*(..)) "
            + "|| execution(public * com.latticeengines.pls.service.impl.MetadataSegmentServiceImpl.delete*(..))")
    public void curdSegment(JoinPoint joinPoint) {
        User user = MultiTenantContext.getUser();
    }
}
