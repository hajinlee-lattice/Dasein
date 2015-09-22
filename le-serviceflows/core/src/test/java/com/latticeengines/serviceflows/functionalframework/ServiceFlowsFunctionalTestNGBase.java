package com.latticeengines.serviceflows.functionalframework;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceflows-context.xml" })
public class ServiceFlowsFunctionalTestNGBase extends AbstractTestNGSpringContextTests {
    
    @Autowired
    private DataTransformationService dataTransformationService;
    
    @Autowired
    private ApplicationContext appContext;
    
    protected Configuration yarnConfiguration = new Configuration();
    
    protected void executeDataFlow(DataFlowContext dataFlowContext, String beanName) throws Exception {
        CascadingDataFlowBuilder dataFlowBuilder = appContext.getBean(beanName, //
                CascadingDataFlowBuilder.class);
        dataFlowBuilder.setLocal(true);
        dataTransformationService.executeNamedTransformation(dataFlowContext, beanName);
    }
    
    protected DataFlowContext createDataFlowContext() {
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("ENGINE", "MR");
        return ctx;
    }
}
