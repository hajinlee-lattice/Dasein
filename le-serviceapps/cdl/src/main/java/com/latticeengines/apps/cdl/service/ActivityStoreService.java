package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.apps.cdl.entitymgr.AtlasStreamEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.StreamDimensionEntityMgr;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.DeriveConfig;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
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

    /**
     * Save given dimension metadata for target tenant
     *
     * @param customerSpace
     *            target tenant
     * @param signature
     *            if not provided, a new signature will be generated
     * @param dimensionMetadataMap
     *            map of streamId -> dimensionName -> metadata
     * @return final signature after combine with tenant namespace, will not be
     *         {@code null}
     */
    String saveDimensionMetadata(@NotNull String customerSpace, String signature,
            @NotNull Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap);

    /**
     * Retrieve dimension metadata of a given stream
     *
     * @param customerSpace
     *            target tenant
     * @param streamName
     *            {@link AtlasStream#getName()} ()} of target stream
     * @param signature
     *            signature of metadata, if not provided, will use the signature
     *            associated to current active version
     * @return map of dimensionName -> metadata, will not be {@code null}
     */
    Map<String, DimensionMetadata> getDimensionMetadataInStream(@NotNull String customerSpace,
            @NotNull String streamName, String signature);

    /**
     * Retrieve all dimension metadata of given tenant
     *
     * @param customerSpace
     *            target tenant
     * @param signature
     *            signature of metadata, if not provided, will use the signature
     *            associated to current active version
     * @return map of streamName -> dimensionName -> metadata, will not be
     *         {@code null}
     */
    Map<String, Map<String, DimensionMetadata>> getDimensionMetadata(@NotNull String customerSpace, String signature);

    /**
     *
     * @param customerSpace
     *          target tenant
     * @param signature
     *          signature of metadata, if not provided, will use the signature
     *          associated to current active version
     * @param withStreamName
     *          if true, will return map of streamName -> dimensionName -> metadata,
     *          if false, will return map of streamId -> dimensionName -> metadata,
     * @return
     */
    Map<String, Map<String, DimensionMetadata>> getDimensionMetadata(@NotNull String customerSpace,
                                                                     String signature, boolean withStreamName);

    /**
     * Retrieve a ActivityMetricsGroup
     *
     * @param customerSpace
     *          target tenant
     * @param groupId
     *          groupId, which is unique within tenant
     * @return
     *          ActivityMetricsGroup
     */
    ActivityMetricsGroup findGroupByGroupId(String customerSpace, String groupId);

    /**
     * Wrapper for {@link DimensionMetadataService#allocateDimensionId(String, Set)}
     *
     * @return map of dimension value -> allocated ID
     */
    Map<String, String> allocateDimensionId(@NotNull String customerSpace, @NotNull Set<String> dimensionValues);

    /**
     * Wrapper for {@link DimensionMetadataService#getDimensionValues(String, Set)}
     *
     * @return map of allocated ID -> dimension value
     */
    Map<String, String> getDimensionValues(@NotNull String customerSpace, @NotNull Set<String> dimensionIds);

    /**
     * Wrapper for {@link DimensionMetadataService#getDimensionIds(String, Set)}
     *
     * @return map of dimension value -> allocated ID
     */
    Map<String, String> getDimensionIds(@NotNull String tenantId, @NotNull Set<String> dimensionValues);

    /**
     * get all streams' IDs and names of a tenant
     *
     * @return streamId -> streamName
     */
    Map<String, String> getStreamNameMap(@NotNull String customerSpace);

    List<AtlasStream> getStreams(@NotNull String customerSpace);

    List<AtlasStream> getStreamsByStreamType(@NotNull String customerSpace, AtlasStream.StreamType streamType);

    Map<AtlasStream.StreamType, List<String>> getStreamTypeToStreamNamesMap(@NotNull String customerSpace);

    boolean addDeriveDimensionConfig(@NotNull String customerSpace, String streamName, DeriveConfig config);
}
