package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SLATermEntityMgr extends BaseEntityMgrRepository<SLATerm, Long> {

    SLATerm findByPid(Long pid);

    SLATerm findByTermNameAndTenant(Tenant tenant, String termName);

    List<SLATerm> findByTenant(Tenant tenant);
}
