package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.activity.Dimension;
import com.latticeengines.domain.exposed.cdl.activity.Stream;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DimensionEntityMgr extends BaseEntityMgrRepository<Dimension, Long> {
    /**
     * Find the unique {@link Dimension} with target name in specific tenant and
     * stream.
     *
     * @param name
     *            target Dimension name
     * @param tenant
     *            target tenant
     * @param stream
     *            target stream
     * @return matching Dimension object, {@code null} if no such Dimension
     *         exists
     */
    Dimension findByNameAndTenantAndStream(@NotNull String name, @NotNull Tenant tenant, @NotNull Stream stream);

    /**
     * Retrieve all Dimension in target tenant
     *
     * @param tenant
     *            target tenant
     * @return list of Dimension, will not be {@code null}
     */
    List<Dimension> findByTenant(@NotNull Tenant tenant);
}
