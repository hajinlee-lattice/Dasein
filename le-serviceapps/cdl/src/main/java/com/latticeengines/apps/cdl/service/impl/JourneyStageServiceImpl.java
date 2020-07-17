package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.JourneyStageEntityMgr;
import com.latticeengines.apps.cdl.service.JourneyStageService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStageUtils;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("JourneyStageService")
public class JourneyStageServiceImpl implements JourneyStageService {

    private static final Logger log = LoggerFactory.getLogger(JourneyStageServiceImpl.class);

    @Inject
    private JourneyStageEntityMgr journeyStageEntityMgr;

    @Override
    public JourneyStage findByPid(String customerSpace, Long pid) {
        return journeyStageEntityMgr.findByPid(pid);
    }

    @Override
    public JourneyStage findByStageName(String customerSpace, String stageName) {
        Tenant tenant = MultiTenantContext.getTenant();
        return journeyStageEntityMgr.findByTenantAndStageName(tenant, stageName);
    }

    @Override
    public List<JourneyStage> findByTenant(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        return journeyStageEntityMgr.findByTenant(tenant);
    }

    @Override
    public JourneyStage createOrUpdate(String customerSpace, JourneyStage journeyStage) {
        JourneyStage newJourneyStage = null;
        if (journeyStage.getPid() != null) {
            newJourneyStage = journeyStageEntityMgr.findByPid(journeyStage.getPid());
        }
        if (newJourneyStage == null) {
            newJourneyStage = new JourneyStage();
        }
        newJourneyStage.setTenant(MultiTenantContext.getTenant());
        newJourneyStage.setPriority(journeyStage.getPriority());
        newJourneyStage.setStageName(journeyStage.getStageName());
        newJourneyStage.setDisplayName(journeyStage.getDisplayName());
        newJourneyStage.setPredicates(journeyStage.getPredicates());
        journeyStageEntityMgr.createOrUpdate(newJourneyStage);
        return newJourneyStage;
    }

    @Override
    public void createDefaultJourneyStages(String customerSpace) {
        createDefaultJourneyStages();
    }

    @Override
    public void delete(String customerSpace, JourneyStage journeyStage) {
        JourneyStage oldJourneyStage = journeyStageEntityMgr.findByPid(journeyStage.getPid());
        if (oldJourneyStage == null) {
            log.error("cannot find JourneyStage in tenant {}, journeyStage name is {}, pid is {}.", customerSpace,
                    journeyStage.getStageName(), journeyStage.getPid());
            return;
        }
        journeyStageEntityMgr.delete(oldJourneyStage);
    }

    private void createDefaultJourneyStages() {
        Tenant tenant = MultiTenantContext.getTenant();
        JourneyStageUtils //
                .atlasJourneyStages(tenant) //
                .forEach(journeyStageEntityMgr::createOrUpdate);
    }
}
