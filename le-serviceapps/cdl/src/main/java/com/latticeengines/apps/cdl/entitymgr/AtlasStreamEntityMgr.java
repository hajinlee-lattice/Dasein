package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.security.Tenant;

public interface AtlasStreamEntityMgr extends BaseEntityMgrRepository<AtlasStream, Long> {
    /**
     * Find the unique {@link AtlasStream} with target name in specific tenant.
     *
     * @param name
     *            target Stream name
     * @param tenant
     *            target tenant
     * @return matching Stream object, {@code null} if no such Stream exists
     */
    AtlasStream findByNameAndTenant(@NotNull String name, @NotNull Tenant tenant);

    /**
     * Find the unique {@link AtlasStream} with target name in specific tenant.
     *
     * @param name
     *            target Stream name
     * @param tenant
     *            target tenant
     * @param inflateDimensions
     *            whether to fetch all associated {@link StreamDimension}s
     * @return matching Stream object, {@code null} if no such Stream exists
     */
    AtlasStream findByNameAndTenant(@NotNull String name, @NotNull Tenant tenant, boolean inflateDimensions);

    /**
     * Retrieve all Stream in target tenant, will not fetch associated
     * {@link StreamDimension}s
     *
     * @param tenant
     *            target tenant
     * @return list of Stream, will not be {@code null}
     */
    List<AtlasStream> findByTenant(@NotNull Tenant tenant);

    /**
     * Retrieve all Stream in target tenant
     *
     * @param tenant
     *            target tenant
     * @param inflateDimensions
     *            whether to fetch all associated {@link StreamDimension}s
     * @return list of Stream, will not be {@code null}
     */
    List<AtlasStream> findByTenant(@NotNull Tenant tenant, boolean inflateDimensions);
}
