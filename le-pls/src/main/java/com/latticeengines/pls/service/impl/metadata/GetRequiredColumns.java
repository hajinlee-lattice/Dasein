package com.latticeengines.pls.service.impl.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.RequiredColumnsExtractor;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component
public abstract class GetRequiredColumns implements RequiredColumnsExtractor {
    
    private static Map<ModelType, RequiredColumnsExtractor> registry = new HashMap<>();
    
    protected GetRequiredColumns(ModelType modelType) {
        registry.put(modelType, this);
    }
    
    @Autowired
    protected ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    protected MetadataProxy metadataProxy;
    
    public static RequiredColumnsExtractor getRequiredColumnsExtractor(String modelId, ModelSummaryEntityMgr mgr) {
        ModelSummary summary = mgr.findByModelId(modelId, false, false, true);
        if (summary == null) {
            return registry.get(ModelType.PYTHONMODEL);
        }
        ModelType modelType = ModelType.getByModelType(summary.getModelType());
        
        if (modelType == null) {
            throw new NullPointerException("Unknown model type " + summary.getModelType());
        }
        
        return registry.get(modelType);
    }

    @Override
    public List<String> getRequiredColumnDisplayNames(String modelId) {
        List<String> requiredColumnDisplayNames = new ArrayList<String>();
        List<Attribute> requiredColumns = getRequiredColumns(modelId);
        for (Attribute column : requiredColumns) {
            requiredColumnDisplayNames.add(column.getDisplayName());
        }
        return requiredColumnDisplayNames;
    }


}

