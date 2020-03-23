package com.latticeengines.auth.exposed.service.impl;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.domain.exposed.pls.GlobalTeamData;

@Component("globalTeamManagementService")
public class GlobalTeamManagementServiceImpl implements GlobalTeamManagementService {

    private static final Logger log = LoggerFactory.getLogger(GlobalTeamManagementServiceImpl.class);

    @Inject
    private GlobalAuthTeamEntityMgr globalAuthTeamEntityMgr;

    @Inject
    private GlobalAuthUserTenantRightEntityMgr globalAuthUserTenantRightEntityMgr;

    @Inject
    private GlobalAuthTenantEntityMgr gaTenantEntityMgr;

    @Override
    public String createTeam(String createdByUser, GlobalTeamData globalTeamData) {
        GlobalAuthTenant tenantData = getGlobalAuthTenant();
        validateTeam(null, globalTeamData, tenantData);
        GlobalAuthTeam globalAuthTeam = new GlobalAuthTeam();
        globalAuthTeam.setName(globalTeamData.getTeamName());
        globalAuthTeam.setCreatedByUser(createdByUser);
        globalAuthTeam.setTeamId(GlobalTeam.generateId());
        globalAuthTeam.setGlobalAuthTenant(tenantData);
        Set<String> teamMembers = globalTeamData.getTeamMembers();
        if (CollectionUtils.isNotEmpty(teamMembers)) {
            List<GlobalAuthUserTenantRight> globalAuthUserTenantRights =
                    globalAuthUserTenantRightEntityMgr.findByEmailsAndTenantId(teamMembers, tenantData.getPid());
            globalAuthTeam.setUserTenantRights(globalAuthUserTenantRights);
        }
        globalAuthTeamEntityMgr.create(globalAuthTeam);
        return globalAuthTeam.getTeamId();
    }

    private void validateTeam(String teamId, GlobalTeamData globalTeamData, GlobalAuthTenant globalAuthTenant) {
        String teamName = globalTeamData.getTeamName() == null ? null : globalTeamData.getTeamName().trim();
        if (StringUtils.isEmpty(teamName)) {
            throw new LedpException(LedpCode.LEDP_18242);
        }
        globalTeamData.setTeamName(teamName);
        GlobalAuthTeam globalAuthTeam = globalAuthTeamEntityMgr.findByTeamNameAndTenantId(globalAuthTenant.getPid(),
                globalTeamData.getTeamName());
        if (globalAuthTeam != null && !globalAuthTeam.getTeamId().equals(teamId)) {
            throw new LedpException(LedpCode.LEDP_18241, new String[]{teamName, globalAuthTenant.getId()});
        }
    }

    @Override
    public void updateTeam(String teamId, GlobalTeamData globalTeamData) {
        GlobalAuthTenant tenantData = getGlobalAuthTenant();
        GlobalAuthTeam globalAuthTeam = globalAuthTeamEntityMgr.findByTeamIdAndTenantId(tenantData.getPid(), teamId);
        if (globalAuthTeam == null) {
            throw new IllegalArgumentException(String.format("cannot find GlobalAuthTeam using teamId %s.", teamId));
        }
        validateTeam(teamId, globalTeamData, tenantData);
        globalAuthTeam.setName(globalTeamData.getTeamName());
        setUserRights(globalAuthTeam, tenantData, globalTeamData.getTeamMembers());
        globalAuthTeamEntityMgr.update(globalAuthTeam);
    }

    private GlobalAuthTenant getTenantData(String tenantId) {
        log.info(String.format("Getting all teams for tenant %s.", tenantId));
        GlobalAuthTenant tenantData = gaTenantEntityMgr.findByTenantId(tenantId);
        if (tenantData == null) {
            throw new IllegalArgumentException(String.format("cannot find GlobalAuthTenant using tenantId %s.", tenantId));
        }
        return tenantData;
    }

    @Override
    public List<GlobalAuthTeam> getTeams(boolean withTeamMember) {
        GlobalAuthTenant tenantData = getGlobalAuthTenant();
        return globalAuthTeamEntityMgr.findByTenantId(tenantData.getPid(), withTeamMember);
    }

    @Override
    public List<GlobalAuthTeam> getTeamsByTeamIds(List<String> teamIds, boolean withTeamMember) {
        GlobalAuthTenant tenantData = getGlobalAuthTenant();
        return globalAuthTeamEntityMgr.findByTeamIdsAndTenantId(tenantData.getPid(), teamIds, withTeamMember);
    }

    @Override
    public List<GlobalAuthTeam> getTeamsByUserName(String username, boolean withTeamMember) {
        GlobalAuthTenant tenantData = getGlobalAuthTenant();
        return globalAuthTeamEntityMgr.findByUsernameAndTenantId(tenantData.getPid(), username, withTeamMember);
    }

    @Override
    public GlobalAuthTeam getTeamById(String teamId, boolean withTeamMember) {
        GlobalAuthTenant tenantData = getGlobalAuthTenant();
        return globalAuthTeamEntityMgr.findByTeamIdAndTenantId(tenantData.getPid(), teamId, withTeamMember);
    }

    @Override
    public Boolean deleteTeamByTeamId(String teamId) {
        GlobalAuthTenant tenantData = getGlobalAuthTenant();
        globalAuthTeamEntityMgr.deleteByTeamId(teamId, tenantData.getPid());
        return true;
    }

    private void setUserRights(GlobalAuthTeam globalAuthTeam, GlobalAuthTenant tenantData, Set<String> teamMembers) {
        if (CollectionUtils.isNotEmpty(teamMembers)) {
            List<GlobalAuthUserTenantRight> globalAuthUserTenantRights =
                    globalAuthUserTenantRightEntityMgr.findByEmailsAndTenantId(teamMembers,
                            tenantData.getPid());
            globalAuthTeam.setUserTenantRights(globalAuthUserTenantRights);
        }
    }

    private GlobalAuthTenant getGlobalAuthTenant() {
        String tenantId = MultiTenantContext.getTenant().getId();
        return getTenantData(tenantId);
    }

    @Override
    public boolean userBelongsToTeam(String username, String teamId) {
        GlobalAuthTenant tenantData = getGlobalAuthTenant();
        return globalAuthTeamEntityMgr.userBelongsToTeam(tenantData.getPid(), username, teamId);
    }
}
