package com.latticeengines.auth.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTeamEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.auth.exposed.service.GlobalTeamManagementService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("globalTeamManagementService")
public class GlobalTeamManagementServiceImpl implements
        GlobalTeamManagementService {

    private static final Logger log = LoggerFactory.getLogger(GlobalTeamManagementServiceImpl.class);

    @Inject
    private GlobalAuthTeamEntityMgr globalAuthTeamEntityMgr;

    @Inject
    private GlobalAuthUserTenantRightEntityMgr globalAuthUserTenantRightEntityMgr;

    @Inject
    private GlobalAuthTenantEntityMgr gaTenantEntityMgr;

    @Override
    public void createTeam(GlobalTeam globalTeam) {
        String tenantId = MultiTenantContext.getCustomerSpace().toString();
        GlobalAuthTenant tenantData = getTenantData(tenantId);
        validateTeam(globalTeam, tenantData);
        GlobalAuthTeam globalAuthTeam = new GlobalAuthTeam();
        globalAuthTeam.setName(globalTeam.getTeamName());
        globalAuthTeam.setCreatedByUser(globalAuthTeam.getCreatedByUser());
        globalAuthTeam.setTeamId(GlobalTeam.generateId());
        Set<String> teamMembers = globalTeam.getTeamMembers();
        if (CollectionUtils.isNotEmpty(teamMembers)) {
            List<GlobalAuthUserTenantRight> globalAuthUserTenantRights =
                    globalAuthUserTenantRightEntityMgr.findByEmailsAndTenantId(teamMembers, tenantData.getPid());
            globalAuthTeam.setUserTenantRights(globalAuthUserTenantRights);
        }
        globalAuthTeamEntityMgr.create(globalAuthTeam);
    }

    private void validateTeam(GlobalTeam globalTeam, GlobalAuthTenant globalAuthTenant) {
        GlobalAuthTeam globalAuthTeam = globalAuthTeamEntityMgr.findByTeamNameAndTenantId(globalAuthTenant.getPid(),
                globalTeam.getTeamName());
        if (globalAuthTeam != null) {
            throw new LedpException(LedpCode.LEDP_18242, new String[]{globalTeam.getTeamName(), globalAuthTenant.getId()});
        }
    }

    @Override
    public void updateTeam(GlobalTeam globalTeam) {
        String tenantId = MultiTenantContext.getCustomerSpace().toString();
        GlobalAuthTenant tenantData = getTenantData(tenantId);
        validateTeam(globalTeam, tenantData);
    }

    private GlobalAuthTenant getTenantData(String tenantId) {
        log.info(String.format("Getting all teams for tenant %s.", tenantId));
        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenantId);
        if (tenantData == null) {
            throw new LedpException(LedpCode.LEDP_18241, new String[]{tenantId});
        }
        return tenantData;
    }

    @Override
    public List<GlobalTeam> getTeams(boolean withTeamMember) {
        String tenantId = MultiTenantContext.getCustomerSpace().toString();
        GlobalAuthTenant tenantData = getTenantData(tenantId);
        List<GlobalAuthTeam> globalAuthTeams = globalAuthTeamEntityMgr.findByTenantId(tenantData.getPid(), withTeamMember);
        return getGlobalTeams(globalAuthTeams, withTeamMember);
    }

    private List<GlobalTeam> getGlobalTeams(List<GlobalAuthTeam> globalAuthTeams, boolean withTeamMember) {
        List<GlobalTeam> globalTeams = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(globalAuthTeams)) {
            for (GlobalAuthTeam globalAuthTeam : globalAuthTeams) {
                GlobalTeam globalTeam = new GlobalTeam();
                globalTeam.setTeamName(globalAuthTeam.getName());
                globalTeam.setTeamId(globalAuthTeam.getTeamId());
                globalTeam.setCreatedByUser(globalAuthTeam.getCreatedByUser());
                if (withTeamMember) {
                    globalTeam.setTeamMembers(globalAuthTeam.getUserTenantRights().stream()
                            .map(globalAuthUserTenantRight -> globalAuthUserTenantRight.getGlobalAuthUser().getEmail()).collect(Collectors.toSet()));
                }
                globalTeams.add(globalTeam);
            }
        }
        return globalTeams;
    }

    @Override
    public List<GlobalTeam> getTeamsByUserName(String username, boolean withTeamMember) {
        String tenantId = MultiTenantContext.getCustomerSpace().toString();
        GlobalAuthTenant tenantData = getTenantData(tenantId);
        List<GlobalAuthTeam> globalAuthTeams =
                globalAuthTeamEntityMgr.findByUsernameAndTenantId(tenantData.getPid(), username, withTeamMember);
        return getGlobalTeams(globalAuthTeams, withTeamMember);
    }
}
