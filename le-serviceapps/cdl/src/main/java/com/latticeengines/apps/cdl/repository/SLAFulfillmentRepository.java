package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.sla.SLAFulfillment;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SLAFulfillmentRepository extends BaseJpaRepository<SLAFulfillment, Long> {

    SLAFulfillment findByPid(@Param("pid") Long pid);

    SLAFulfillment findByActionAndTerm(@Param("action") Action action, @Param("term") SLATerm term);

    List<SLAFulfillment> findByAction(@Param("action") Action action);

    List<SLAFulfillment> findByTerm(@Param("term") SLATerm term);

    List<SLAFulfillment> findByTenant(@Param("tenant") Tenant tenant);
}
