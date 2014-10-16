package com.latticeengines.dataflow.exposed.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import cascading.flow.Flow;
import cascading.flow.FlowDef;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.exception.DataFlowException;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component("dataTransformationService")
public class DataTransformationServiceImpl implements DataTransformationService {

    @Autowired
    private ApplicationContext appContext;
    
    @Override
    public void executeNamedTransformation(DataFlowContext dataFlowCtx, String dataFlowBldrBeanName) {
        Object dataFlowBldrBean = appContext.getBean(dataFlowBldrBeanName);
        
        if (!(dataFlowBldrBean instanceof CascadingDataFlowBuilder)) {
            throw new DataFlowException();
        }
        
        CascadingDataFlowBuilder dataFlow = (CascadingDataFlowBuilder) dataFlowBldrBean;
        
        @SuppressWarnings("unchecked")
        Map<String, String> sourceTables = dataFlowCtx.getProperty("SOURCES", Map.class);
        
        FlowDef flowDef = dataFlow.constructFlowDefinition(sourceTables);
        Flow<?> flow = dataFlow.build(flowDef);
        flow.complete();
    }

}
