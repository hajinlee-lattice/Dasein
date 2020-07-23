package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.SLATermEntityMgr;
import com.latticeengines.apps.cdl.service.SLATermService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("SLATermService")
public class SLATermServiceImpl implements SLATermService {

    private static final Logger log = LoggerFactory.getLogger(SLATermServiceImpl.class);

    @Inject
    private SLATermEntityMgr slaTermEntityMgr;

    @Override
    public SLATerm findByPid(@NotNull String customerSpace, @NotNull Long pid) {
        Preconditions.checkArgument(pid != null, "pid is empty.");
        return slaTermEntityMgr.findByPid(pid);
    }

    @Override
    public SLATerm findByTermName(@NotNull String customerSpace, @NotNull String termName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(termName), "termName is empty.");
        Tenant tenant = MultiTenantContext.getTenant();
        return slaTermEntityMgr.findByTermNameAndTenant(tenant, termName);
    }

    @Override
    public List<SLATerm> findByTenant(@NotNull String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        return slaTermEntityMgr.findByTenant(tenant);
    }

    @Override
    public SLATerm createOrUpdate(String customerSpace, SLATerm slaTerm) {
        Preconditions.checkArgument(slaTerm != null, "can't create empty slaTerm.");
        SLATerm newSlaTerm = null;
        if (slaTerm.getPid() != null) {
            newSlaTerm = slaTermEntityMgr.findByPid(slaTerm.getPid());
        }
        if (newSlaTerm == null) {
            newSlaTerm = new SLATerm();
        }
        newSlaTerm.setTenant(MultiTenantContext.getTenant());
        newSlaTerm.setTermName(slaTerm.getTermName());
        newSlaTerm.setVersion(slaTerm.getVersion());
        newSlaTerm.setFeatureFlags(slaTerm.getFeatureFlags());
        newSlaTerm.setSlaTermType(slaTerm.getSlaTermType());
        newSlaTerm.setTimeZone(slaTerm.getTimeZone());
        newSlaTerm.setPredicates(slaTerm.getPredicates());
        newSlaTerm.setScheduleCron(slaTerm.getScheduleCron());
        newSlaTerm.setDeliveryDuration(slaTerm.getDeliveryDuration());
        slaTermEntityMgr.createOrUpdate(newSlaTerm);
        return newSlaTerm;
    }

    @Override
    public void delete(String customerSpace, SLATerm slaTerm) {
        Preconditions.checkArgument(slaTerm != null, "can't give empty slaTerm.");
        Preconditions.checkArgument(slaTerm.getPid() != null, "pid is empty.");
        SLATerm oldSlaTerm = slaTermEntityMgr.findByPid(slaTerm.getPid());
        if (oldSlaTerm == null) {
            log.error("cannot find SLATerm in tenant {}, SLATerm name is {}, pid is {}.", customerSpace,
                    slaTerm.getTermName(), slaTerm.getPid());
            return;
        }
        slaTermEntityMgr.delete(oldSlaTerm);
    }

}
