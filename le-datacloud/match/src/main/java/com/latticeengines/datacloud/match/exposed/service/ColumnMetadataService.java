package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public interface ColumnMetadataService extends HasDataCloudVersion {

    List<ColumnMetadata> fromPredefinedSelection(Predefined selectionName, String dataCloudVersion);

    List<ColumnMetadata> fromSelection(ColumnSelection selection, String dataCloudVersion);

    List<ColumnMetadata> fromSelectionUpdated(ColumnSelection selection, String dataCloudVersion);

    Schema getAvroSchema(Predefined selectionName, String recordName, String dataCloudVersion);

    Schema getAvroSchemaFromColumnMetadatas(List<ColumnMetadata> columnMetadatas, String recordName,
            String dataCloudVersion);

    List<ColumnMetadata> findAll(String dataCloudVersion);

    void updateColumnMetadatas(String dataCloudVersion, List<ColumnMetadata> columnMetadatas);

}
