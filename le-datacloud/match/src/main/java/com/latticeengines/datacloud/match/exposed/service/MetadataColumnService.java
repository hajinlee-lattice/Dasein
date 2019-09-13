package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

import reactor.core.publisher.ParallelFlux;

public interface MetadataColumnService<E extends MetadataColumn> extends HasDataCloudVersion {

    /**
     * Find MetadataColumn for specific datacloud version and filtered by tag
     * containing predefined selection name
     *
     * Lookup sequence: cache -> S3
     *
     * @param selectName:
     *            predefined column selection
     * @param dataCloudVersion:
     *            datacloud version
     * @return
     */
    List<E> findByColumnSelection(Predefined selectName, String dataCloudVersion);

    E getMetadataColumn(String columnId, String dataCloudVersion);

    List<E> getMetadataColumns(List<String> columnIds, String dataCloudVersion);

    ParallelFlux<E> scan(String dataCloudVersion, Integer page, Integer size);

    Long count(String dataCloudVersion);

    /**
     * Publish DataCloud metadata of designated version to S3. Application warms
     * up metadata cache by reading from S3 instead of MySQL to avoid DB
     * connection congestion
     *
     * This API is only triggered manually when datacloud metadata is refreshed;
     * Always delete first and then re-publish
     *
     * @param dataCloudVersion:
     *            datacloud version
     * @return #rows published
     */
    long s3Publish(String dataCloudVersion);

    /**
     * Get all the MetadataColumns of specific datacloud version. This method is
     * to serve upper-layer metadata related cache.
     *
     * Lookup sequence: cache -> S3
     *
     * @param dataCloudVersion:
     *            datacloud version
     * @return
     */
    List<E> getMetadataColumns(String dataCloudVersion);

}
