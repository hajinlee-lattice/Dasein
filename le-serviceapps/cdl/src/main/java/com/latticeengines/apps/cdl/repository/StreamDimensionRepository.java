package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.security.Tenant;


public interface StreamDimensionRepository extends BaseJpaRepository<StreamDimension, Long> {

    List<StreamDimension> findByNameAndTenantAndStream(String name, Tenant tenant, AtlasStream stream);

    List<StreamDimension> findByTenant(Tenant tenant);
}
