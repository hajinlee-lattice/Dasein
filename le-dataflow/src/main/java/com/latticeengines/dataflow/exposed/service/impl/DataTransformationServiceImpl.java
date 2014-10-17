package com.latticeengines.dataflow.exposed.service.impl;

import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;

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
            throw new DataFlowException("Builder bean not instance of builder.");
        }
        
        CascadingDataFlowBuilder dataFlow = (CascadingDataFlowBuilder) dataFlowBldrBean;
        
        @SuppressWarnings("unchecked")
        Map<String, String> sourceTables = dataFlowCtx.getProperty("SOURCES", Map.class);
        
        String lastOperator = dataFlow.constructFlowDefinition(sourceTables);
        
        @SuppressWarnings("deprecation")
        Tap<?, ?, ?> sink = new Lfs(new TextDelimited(), "/tmp/EventTable", true);
        Properties properties = new Properties();
        properties.put("mapred.job.queue.name", "Priority0.MapReduce.0");
        AppProps.setApplicationJarClass(properties, dataFlow.getClass());
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        FlowDef flowDef = FlowDef.flowDef().setName("wc") //
                .addSources(dataFlow.getSources()) //
                .addTailSink(dataFlow.getPipeByName(lastOperator), sink);
 
        Flow<?> flow = flowConnector.connect(flowDef);
        flow.writeDOT("dot/wcr.dot");
        flow.complete();

    }

}
