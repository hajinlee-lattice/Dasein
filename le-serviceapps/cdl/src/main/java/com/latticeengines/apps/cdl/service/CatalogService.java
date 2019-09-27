package com.latticeengines.apps.cdl.service;

import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;

public interface CatalogService {

    /**
     * Create given catalog under tenant with target customerSpace.
     *
     * @param customerSpace
     *            target tenant
     * @param catalogName
     *            catalog name, must be provided
     * @param taskUniqueId
     *            unique id for associated {@link DataFeedTask}, can be optional
     * @return created catalog, will not be {@code null}
     */
    Catalog create(@NotNull String customerSpace, @NotNull String catalogName, String taskUniqueId);

    /**
     * Wrapper for {@link CatalogEntityMgr#findByNameAndTenant(String, Tenant)} to
     * check tenant with target customerSpace exists
     */
    Catalog findByTenantAndName(@NotNull String customerSpace, @NotNull String catalogName);
}
