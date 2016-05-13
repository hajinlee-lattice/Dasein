package com.latticeengines.dataplatform.service;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.modeling.Model;

public abstract class ModelValidationService {
    
    private static Map<String, ModelValidationService> map = new HashMap<>();
    
    public ModelValidationService(String[] keys) {
        for (String key : keys) {
            map.put(key, this);
        }
        
    }
    
    public static ModelValidationService get(String algorithmName) {
        return map.get(algorithmName);
    }
    

    public abstract void validate(Model model) throws Exception;
}
