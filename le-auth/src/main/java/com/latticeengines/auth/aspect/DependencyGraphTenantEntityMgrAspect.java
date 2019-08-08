package com.latticeengines.auth.aspect;

import javax.inject.Inject;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.graph.VertexCreationRequest;
import com.latticeengines.domain.exposed.graph.VertexDeletionRequest;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.graphdb.entity.GraphEntityManager;

@Aspect
public class DependencyGraphTenantEntityMgrAspect {

    private static final Logger log = LoggerFactory.getLogger(DependencyGraphTenantEntityMgrAspect.class);

    @Inject
    private GraphEntityManager graphEntityManager;

    @Before("execution(* com.latticeengines.auth.exposed.entitymanager.impl.GlobalAuthTenantEntityMgrImpl.create*(..))")
    public void create(JoinPoint joinPoint) throws Exception {
        log.info(String.format("Received a call with arg type=%s, value=%s", joinPoint.getArgs()[0].getClass(),
                JsonUtils.serialize(joinPoint.getArgs()[0])));
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof GlobalAuthTenant) {
            GlobalAuthTenant tenant = (GlobalAuthTenant) joinPoint.getArgs()[0];

            VertexCreationRequest request = new VertexCreationRequest();
            request.setObjectId(tenant.getId());
            request.setType(VertexType.TENANT);
            graphEntityManager.addVertex(tenant.getId(), null, null, null, request);
        }
    }

    @Before("execution(* com.latticeengines.auth.exposed.entitymanager.impl.GlobalAuthTenantEntityMgrImpl.delete*(..))")
    public void delete(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof GlobalAuthTenant) {
            GlobalAuthTenant tenant = (GlobalAuthTenant) joinPoint.getArgs()[0];

            VertexDeletionRequest request = new VertexDeletionRequest();
            request.setObjectId(tenant.getId());
            request.setType(VertexType.TENANT);
            request.setForceDelete(true);
            graphEntityManager.dropVertex(tenant.getId(), null, null, null, request);
        }
    }
}
