package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Attribute;

public interface ModelService {

    List<String> getRequiredColumnDisplayNames(String modelId);

    List<Attribute> getRequiredColumns(String modelId);

    boolean copyModel(ModelSummary modelSummary, String sourceTenantId, String targetTenantId);
    
    

}
