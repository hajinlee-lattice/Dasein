package com.latticeengines.apps.cdl.service;

import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StreamDimensionEntityMgr;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ActivityStoreService {

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
    Catalog createCatalog(@NotNull String customerSpace, @NotNull String catalogName, String taskUniqueId);

    /**
     * Wrapper for {@link CatalogEntityMgr#findByNameAndTenant(String, Tenant)} to
     * check tenant with target customerSpace exists
     */
    Catalog findCatalogByTenantAndName(@NotNull String customerSpace, @NotNull String catalogName);

    /**
     * Create given stream and attached dimensions.
     *
     * @param customerSpace
     *            target tenant
     * @param stream
     *            stream object to be created
     * @return created stream, will not be {@code null}
     */
    AtlasStream createStream(@NotNull String customerSpace, @NotNull AtlasStream stream);

    /**
     * Wrapper for
     * {@link AtlasStreamEntityMgr#findByNameAndTenant(String, Tenant, boolean)} to
     * check tenant with target customerSpace exists
     */
    AtlasStream findStreamByTenantAndName(@NotNull String customerSpace, @NotNull String streamName,
            boolean inflateDimensions);

    /**
     * Wrapper for {@link StreamDimensionEntityMgr#update(Object)}
     */
    void updateStreamDimension(@NotNull String customerSpace, @NotNull String streamName,
            @NotNull StreamDimension dimension);
}
