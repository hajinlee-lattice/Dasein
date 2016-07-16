package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.RequiredColumnsExtractor;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;

public interface ModelMetadataService extends RequiredColumnsExtractor {
    List<VdbMetadataField> getMetadata(String modelId);

    Table cloneTrainingTable(String modelSummaryId);

    Table getTrainingTableFromModelId(String modelId);

    List<Attribute> getAttributesFromFields(List<Attribute> attributes, List<VdbMetadataField> fields);

    Table getEventTableFromModelId(String modelId);
}
