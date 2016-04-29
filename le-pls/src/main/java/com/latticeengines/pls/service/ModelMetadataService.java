package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;

public interface ModelMetadataService {
    List<VdbMetadataField> getMetadata(String modelId);

    Table cloneAndUpdateMetadata(String modelSummaryId, List<VdbMetadataField> fields);

    List<String> getRequiredColumnDisplayNames(String modelId);

    List<Attribute> getRequiredColumns(String modelId);

    Table getTrainingTableFromModelId(String modelId);
}
