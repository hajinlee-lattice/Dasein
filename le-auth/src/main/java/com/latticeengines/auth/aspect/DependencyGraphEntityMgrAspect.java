package com.latticeengines.auth.aspect;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.graph.VertexCreationRequest;
import com.latticeengines.domain.exposed.graph.VertexDeletionRequest;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.graphdb.entity.GraphEntityManager;

@Aspect
public class DependencyGraphEntityMgrAspect {

    private static final Logger log = LoggerFactory.getLogger(DependencyGraphEntityMgrAspect.class);

    @Inject
    private GraphEntityManager graphEntityManager;

    @Inject
    private GlobalAuthDependenciesToGraphAction globalAuthDependenciesToGraphAction;

    @Before("execution(* com.latticeengines.auth.exposed.entitymanager.impl.GlobalAuthTenantEntityMgrImpl.create*(..))")
    public void createTennat(JoinPoint joinPoint) throws Exception {
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
    public void deleteTenant(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof GlobalAuthTenant) {
            GlobalAuthTenant tenant = (GlobalAuthTenant) joinPoint.getArgs()[0];
            VertexDeletionRequest request = new VertexDeletionRequest();
            request.setObjectId(tenant.getId());
            request.setType(VertexType.TENANT);
            request.setForceDelete(true);
            graphEntityManager.dropVertex(tenant.getId(), null, null, null, request);
        }
    }

    @Before("execution(* com.latticeengines.auth.exposed.entitymanager.impl.GlobalAuthTeamEntityMgrImpl.create*(..))")
    public void createTeam(JoinPoint joinPoint) throws Exception {
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof GlobalAuthTeam) {
            GlobalAuthTeam globalAuthTeam = (GlobalAuthTeam) joinPoint.getArgs()[0];
            log.info(String.format("Received a call with arg type=%s, value=%s", globalAuthTeam.getClass(), globalAuthTeam.getTeamId()));
            globalAuthDependenciesToGraphAction.createTeamVertex(globalAuthTeam);
        }
    }

    @Around("execution(* com.latticeengines.auth.exposed.entitymanager.impl.GlobalAuthTeamEntityMgrImpl.delete*(..))")
    public void deleteTeam(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getArgs().length > 1) {
            String teamId = null;
            if (joinPoint.getArgs()[0] instanceof GlobalAuthTeam) {
                teamId = ((GlobalAuthTeam) joinPoint.getArgs()[0]).getTeamId();
            } else if (joinPoint.getArgs()[0] instanceof String) {
                teamId = (String) joinPoint.getArgs()[0];
            }
            if (StringUtils.isNotEmpty(teamId)) {
                globalAuthDependenciesToGraphAction.checkDeleteSafety(teamId, VertexType.TEAM);
                joinPoint.proceed(joinPoint.getArgs());
                globalAuthDependenciesToGraphAction.deleteVertex(MultiTenantContext.getTenant().getId(), teamId, VertexType.TEAM);
            }
        }
    }
}
