package com.latticeengines.apps.cdl.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.security.Tenant;

public interface CatalogRepository extends BaseJpaRepository<Catalog, Long> {

    List<Catalog> findByNameAndTenant(String name, Tenant tenant);

    List<Catalog> findByTenant(Tenant tenant);
}
