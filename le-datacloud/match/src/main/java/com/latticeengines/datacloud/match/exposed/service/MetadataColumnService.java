package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

import reactor.core.publisher.ParallelFlux;

public interface MetadataColumnService<E extends MetadataColumn> extends HasDataCloudVersion {

    List<E> findByColumnSelection(Predefined selectName, String dataCloudVersion);

    E getMetadataColumn(String columnId, String dataCloudVersion);

    List<E> getMetadataColumns(List<String> columnIds, String dataCloudVersion);

    ParallelFlux<E> scan(String dataCloudVersion, Integer page, Integer size);

    Long count(String dataCloudVersion);

}
