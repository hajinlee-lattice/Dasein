package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SLATermRepository extends BaseJpaRepository<SLATerm, Long> {

    SLATerm findByPid(@Param("pid") Long pid);

    SLATerm findByTermNameAndTenant(@Param("tenant") Tenant tenant, @Param("termName") String termName);

    List<SLATerm> findByTenant(@Param("tenant") Tenant tenant);
}
