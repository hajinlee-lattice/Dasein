package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.service.TransformerService;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;

@Component("transformerService")
public class TransformerServiceImpl implements TransformerService {

    @Autowired
    private List<Transformer> transformerList;

    private Map<String, Transformer> transformerMap = null;

    @PostConstruct
    private void postConstruct() {
        transformerMap = new HashMap<>();
        for (Transformer transformer: transformerList) {
            transformerMap.put(transformer.getName(), transformer);
        }
    }

    @Override
    public Transformer findTransformerByName(String name) {
        Transformer transformer =  transformerMap.get(name);
        return transformer;
    }

    @Override
    public List<Transformer> getTransformers() {
        return transformerList;
    }
}
