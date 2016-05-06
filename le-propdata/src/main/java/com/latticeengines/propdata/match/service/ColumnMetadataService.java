package com.latticeengines.propdata.match.service;

import java.util.List;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public interface ColumnMetadataService {
    List<ColumnMetadata> fromPredefinedSelection (ColumnSelection.Predefined selectionName);
    Schema getAvroSchema(ColumnSelection.Predefined selectionName, String recordName);
}
