package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.Stream;
import com.latticeengines.domain.exposed.security.Tenant;

public interface StreamRepository extends BaseJpaRepository<Stream, Long> {

    List<Stream> findByNameAndTenant(String name, Tenant tenant);

    List<Stream> findByTenant(Tenant tenant);
}
