package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.security.Tenant;

public interface CatalogEntityMgr extends BaseEntityMgrRepository<Catalog, Long> {

    List<Catalog> findByNameAndTenant(@NotNull String name, @NotNull Tenant tenant);

    List<Catalog> findByTenant(@NotNull Tenant tenant);
}
