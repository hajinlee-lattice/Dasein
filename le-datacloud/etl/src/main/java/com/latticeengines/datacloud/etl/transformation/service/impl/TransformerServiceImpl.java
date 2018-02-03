package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlow;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformerService;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableDataflowTransformer;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("transformerService")
public class TransformerServiceImpl implements TransformerService {

    @Inject
    private List<Transformer> transformerList;

    @Inject
    private List<ConfigurableFlow> configurableFlows;

    @Inject
    protected SimpleTransformationDataFlowService dataFlowService;

    @Inject
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Inject
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private Configuration yarnConfiguration;

    private Map<String, Transformer> transformerMap;

    @PostConstruct
    private void postConstruct() {
        transformerMap = new HashMap<>();
        for (Transformer transformer: transformerList) {
            transformerMap.put(transformer.getName(), transformer);
        }

        for (ConfigurableFlow flow : configurableFlows) {
            if (transformerMap.containsKey(flow.getTransformerName())) {
                continue;
            }
            ConfigurableDataflowTransformer transformer = new ConfigurableDataflowTransformer();
            transformer.setConfigClass(flow.getTransformerConfigClass());
            transformer.setName(flow.getTransformerName());
            transformer.setSourceColumnEntityMgr(sourceColumnEntityMgr);
            transformer.setDataFlowBeanName(flow.getDataFlowBeanName());
            transformer.setDataFlowService(dataFlowService);
            transformer.setHdfsSourceEntityMgr(hdfsSourceEntityMgr);
            transformer.setMetadataProxy(metadataProxy);
            transformer.setYarnConfiguration(yarnConfiguration);
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
