package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public interface ColumnMetadataService {
    List<ColumnMetadata> fromPredefinedSelection(Predefined selectionName, String dataCloudVersion);

    List<ColumnMetadata> fromSelection(ColumnSelection selection, String dataCloudVersion);

    Schema getAvroSchema(Predefined selectionName, String recordName, String dataCloudVersion);

    Schema getAvroSchemaFromColumnMetadatas(List<ColumnMetadata> columnMetadatas, String recordName, String dataCloudVersion);
}
