package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Attribute;

public interface RequiredColumnsExtractor {

    List<String> getRequiredColumnDisplayNames(String modelId);

    List<Attribute> getRequiredColumns(String modelId);

}
