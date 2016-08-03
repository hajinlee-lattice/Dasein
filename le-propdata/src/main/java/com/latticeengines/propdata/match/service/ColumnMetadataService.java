package com.latticeengines.propdata.match.service;

import java.util.List;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public interface ColumnMetadataService {
    List<ColumnMetadata> fromPredefinedSelection (ColumnSelection.Predefined selectionName, String dataCloudVersion);
    List<ColumnMetadata> fromSelection(ColumnSelection selection, String dataCloudVersion);
    Schema getAvroSchema(ColumnSelection.Predefined selectionName, String recordName, String dataCloudVersion);
}
