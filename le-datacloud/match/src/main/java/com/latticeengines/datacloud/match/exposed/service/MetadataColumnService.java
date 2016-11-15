package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public interface MetadataColumnService<E extends MetadataColumn> extends HasDataCloudVersion {

    List<E> findByColumnSelection(Predefined selectName, String dataCloudVersion);

    E getMetadataColumn(String columnId, String dataCloudVersion);

    List<E> getMetadataColumns(List<String> columnIds, String dataCloudVersion);

    List<E> scan(String dataCloudVersion);

    void updateMetadataColumns(String dataCloudVersion, List<ColumnMetadata> columnMetadatas);

}
