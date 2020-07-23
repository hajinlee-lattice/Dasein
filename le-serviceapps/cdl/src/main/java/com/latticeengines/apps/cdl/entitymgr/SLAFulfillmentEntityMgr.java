package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.sla.SLAFulfillment;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SLAFulfillmentEntityMgr extends BaseEntityMgrRepository<SLAFulfillment, Long> {

    SLAFulfillment findByPid(Long pid);

    SLAFulfillment findByActionAndTerm(Action action, SLATerm term);

    List<SLAFulfillment> findByAction(Action action);

    List<SLAFulfillment> findByTerm(SLATerm term);

    List<SLAFulfillment> findByTenant(Tenant tenant);
}
