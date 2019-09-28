package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.security.Tenant;

public interface AtlasStreamRepository extends BaseJpaRepository<AtlasStream, Long> {

    List<AtlasStream> findByNameAndTenant(String name, Tenant tenant);

    List<AtlasStream> findByTenant(Tenant tenant);
}
