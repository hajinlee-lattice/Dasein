package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.Dimension;
import com.latticeengines.domain.exposed.cdl.activity.Stream;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DimensionRepository extends BaseJpaRepository<Dimension, Long> {

    List<Dimension> findByNameAndTenantAndStream(InterfaceName name, Tenant tenant, Stream stream);

    List<Dimension> findByTenant(Tenant tenant);
}
