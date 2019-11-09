package com.latticeengines.apps.cdl.service;

import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;

public interface DimensionMetadataService {

    /**
     * Upsert metadata of a given dimension
     *
     * @param signature
     *            namespace for this metadata, typically contains tenant info
     * @param streamId
     *            {@link AtlasStream#getStreamId()}
     * @param dimensionName
     *            {@link StreamDimension#getName()}
     * @param metadata
     *            metadata to save
     */
    void put(@NotNull String signature, @NotNull String streamId, @NotNull String dimensionName,
            @NotNull DimensionMetadata metadata);

    /**
     * Set all metadata in a signature (upsert each dimension separately)
     *
     * @param signature
     *            namespace for this metadata, typically contains tenant info
     * @param dimensionMetadataMap
     *            map of streamId -> dimensionName -> metadata
     */
    void put(@NotNull String signature, @NotNull Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap);

    /**
     * Retrieve metadata for a single dimension
     * 
     * @param signature
     *            namespace for this metadata, typically contains tenant info
     * @param streamId
     *            {@link AtlasStream#getStreamId()}
     * @param dimensionName
     *            {@link StreamDimension#getName()}
     * @return metadata instance, {@code null} if not exist
     */
    DimensionMetadata get(@NotNull String signature, @NotNull String streamId, @NotNull String dimensionName);

    /**
     * Retrieve all metadata for a target {@link AtlasStream}
     *
     * @param signature
     *            namespace for this metadata, typically contains tenant info
     * @param streamId
     *            {@link AtlasStream#getStreamId()}
     * @return map of dimensionName -> metadata, will not be {@code null}
     */
    Map<String, DimensionMetadata> getMetadataInStream(@NotNull String signature, @NotNull String streamId);

    /**
     * Retrieve all metadata in the namespace
     *
     * @param signature
     *            namespace for this metadata, typically contains tenant info
     * @return map of streamId -> dimensionName -> metadata, will not be
     *         {@code null}
     */
    Map<String, Map<String, DimensionMetadata>> getMetadata(@NotNull String signature);

    /**
     * Delete all metadata in the namespace
     *
     * @param signature
     *            namespace for this metadata, typically contains tenant info
     */
    void delete(@NotNull String signature);
}
