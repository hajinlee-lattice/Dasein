package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;

public interface ModelMetadataService {
    List<VdbMetadataField> getMetadata(String modelId);

    Table cloneTrainingTable(String modelSummaryId);

    List<String> getRequiredColumnDisplayNames(String modelId);

    List<Attribute> getRequiredColumns(String modelId);

    Table getTrainingTableFromModelId(String modelId);

    List<Attribute> getAttributesFromFields(List<Attribute> attributes, List<VdbMetadataField> fields);
}
