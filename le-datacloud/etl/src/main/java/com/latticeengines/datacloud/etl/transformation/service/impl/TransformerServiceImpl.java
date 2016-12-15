package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlow;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformerService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableDataflowTransformer;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;

@Component("transformerService")
public class TransformerServiceImpl implements TransformerService {

    @Autowired
    private List<Transformer> transformerList;

    @Autowired
    private List<ConfigurableFlow> configurableFlows;

    @Autowired
    protected SimpleTransformationDataFlowService dataFlowService;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    private Map<String, Transformer> transformerMap;

    @PostConstruct
    private void postConstruct() {
        transformerMap = new HashMap<>();
        for (Transformer transformer: transformerList) {
            transformerMap.put(transformer.getName(), transformer);
        }

        for (ConfigurableFlow flow : configurableFlows) {
            ConfigurableDataflowTransformer transformer = new ConfigurableDataflowTransformer();
            transformer.setConfigClass(flow.getTransformerConfigClass());
            transformer.setName(flow.getTransformerName());
            transformer.setSourceColumnEntityMgr(sourceColumnEntityMgr);
            transformer.setDataFlowBeanName(flow.getDataFlowBeanName());
            transformer.setDataFlowService(dataFlowService);
            transformerList.add(transformer);
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
