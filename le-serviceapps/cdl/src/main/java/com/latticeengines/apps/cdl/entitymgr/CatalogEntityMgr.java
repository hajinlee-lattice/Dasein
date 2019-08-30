package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.security.Tenant;

public interface CatalogEntityMgr extends BaseEntityMgrRepository<Catalog, Long> {

    /**
     * Find the unique {@link Catalog} with target name in specific tenant.
     *
     * @param name
     *            target catalog name
     * @param tenant
     *            target tenant
     * @return matching catalog object, {@code null} if no such catalog exists
     */
    Catalog findByNameAndTenant(@NotNull String name, @NotNull Tenant tenant);

    /**
     * Retrieve all catalog in target tenant
     *
     * @param tenant
     *            target tenant
     * @return list of catalog, will not be {@code null}
     */
    List<Catalog> findByTenant(@NotNull Tenant tenant);
}
