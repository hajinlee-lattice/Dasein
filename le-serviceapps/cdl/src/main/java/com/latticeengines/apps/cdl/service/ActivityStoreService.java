package com.latticeengines.apps.cdl.service;

import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
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
     * @param primaryKeyColumn
     *            column name used to uniquely identify catalog entry
     * @return created catalog, will not be {@code null}
     */
    Catalog createCatalog(@NotNull String customerSpace, @NotNull String catalogName, String taskUniqueId,
            String primaryKeyColumn);

    /*-
     * Wrappers to check tenant with target customerSpace exists
     */

    Catalog findCatalogByTenantAndName(@NotNull String customerSpace, @NotNull String catalogName);

    Catalog findCatalogByIdAndName(@NotNull String customerSpace, @NotNull String catalogId);

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
     * Wrapper for {@link StreamDimensionEntityMgr#update(Object)}, return the
     * updated dimension
     */
    StreamDimension updateStreamDimension(@NotNull String customerSpace, @NotNull String streamName,
            @NotNull StreamDimension dimension);
}
