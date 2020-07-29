package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.SLAFulfillmentEntityMgr;
import com.latticeengines.apps.cdl.service.SLAFulfillmentService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.sla.SLAFulfillment;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("SLAFulfillmentService")
public class SLAFulfillmentServiceImpl implements SLAFulfillmentService {

    private static final Logger log = LoggerFactory.getLogger(SLAFulfillmentServiceImpl.class);

    @Inject
    private SLAFulfillmentEntityMgr slaFulfillmentEntityMgr;

    @Override
    public SLAFulfillment findByPid(@NotNull String customerSpace, @NotNull Long pid) {
        return slaFulfillmentEntityMgr.findByPid(pid);
    }

    @Override
    public SLAFulfillment findByActionAndTerm(String customerSpace, Action action, SLATerm term) {
        Tenant tenant = MultiTenantContext.getTenant();
        Preconditions.checkArgument(checkAction(action, tenant), "action has wrong tenant.");
        Preconditions.checkArgument(checkTerm(term, tenant), "slaTerm has wrong tenant.");
        return slaFulfillmentEntityMgr.findByActionAndTerm(action, term);
    }

    @Override
    public List<SLAFulfillment> findByAction(String customerSpace, Action action) {
        Tenant tenant = MultiTenantContext.getTenant();
        Preconditions.checkArgument(checkAction(action, tenant), "action has wrong tenant.");
        return slaFulfillmentEntityMgr.findByAction(action);
    }

    @Override
    public List<SLAFulfillment> findByTerm(String customerSpace, SLATerm term) {
        Tenant tenant = MultiTenantContext.getTenant();
        Preconditions.checkArgument(checkTerm(term, tenant), "slaTerm has wrong tenant.");
        return slaFulfillmentEntityMgr.findByTerm(term);
    }

    @Override
    public List<SLAFulfillment> findByTenant(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        return slaFulfillmentEntityMgr.findByTenant(tenant);
    }

    @Override
    public SLAFulfillment createOrUpdate(String customerSpace, SLAFulfillment slaFulfillment) {
        Preconditions.checkArgument(slaFulfillment != null, "can't create empty SLAFulfillment.");
        SLAFulfillment fulfillment = null;
        if (slaFulfillment.getPid() != null) {
            fulfillment = slaFulfillmentEntityMgr.findByPid(slaFulfillment.getPid());
        }
        if (fulfillment == null) {
            fulfillment = new SLAFulfillment();
        }
        Tenant tenant = MultiTenantContext.getTenant();
        Preconditions.checkArgument(slaFulfillment.getAction() != null && slaFulfillment.getAction().getTenant() != null && slaFulfillment.getAction().getTenant().equals(tenant),
                "action has wrong tenant.");
        Preconditions.checkArgument(slaFulfillment.getTerm() != null && slaFulfillment.getTerm().getTenant() != null && slaFulfillment.getTerm().getTenant().equals(tenant),
                "slaTerm has wrong tenant.");
        fulfillment.setAction(slaFulfillment.getAction());
        fulfillment.setTerm(slaFulfillment.getTerm());
        fulfillment.setTenant(tenant);
        fulfillment.setDeliveredTime(slaFulfillment.getDeliveredTime());
        fulfillment.setFulfillmentStatus(slaFulfillment.getFulfillmentStatus());
        fulfillment.setJobFailedTime(slaFulfillment.getJobFailedTime());
        fulfillment.setJobStartTime(slaFulfillment.getJobStartTime());
        fulfillment.setDeliveryDeadline(slaFulfillment.getDeliveryDeadline());
        fulfillment.setEarliestKickoffTime(slaFulfillment.getEarliestKickoffTime());
        fulfillment.setSlaVersion(slaFulfillment.getSlaVersion());
        slaFulfillmentEntityMgr.createOrUpdate(fulfillment);
        return fulfillment;
    }

    @Override
    public void delete(String customerSpace, SLAFulfillment slaFulfillment) {
        Preconditions.checkArgument(slaFulfillment != null, "can't give empty slaTerm.");
        Preconditions.checkArgument(slaFulfillment.getPid() != null, "pid is empty.");
        SLAFulfillment oldSlaFulfillment = slaFulfillmentEntityMgr.findByPid(slaFulfillment.getPid());
        if (oldSlaFulfillment == null) {
            log.error("cannot find SLAFulfillment in tenant {}, pid is {}.", customerSpace,
                    slaFulfillment.getPid());
            return;
        }
        slaFulfillmentEntityMgr.delete(oldSlaFulfillment);
    }

    private boolean checkAction(Action action, Tenant tenant) {
        return action != null && action.getTenant() != null && action.getTenant().equals(tenant);
    }

    private boolean checkTerm(SLATerm term, Tenant tenant) {
        return (term != null && term.getTenant() != null && term.getTenant().equals(tenant)) || (term != null && term.getTenant() == null);
    }
}
