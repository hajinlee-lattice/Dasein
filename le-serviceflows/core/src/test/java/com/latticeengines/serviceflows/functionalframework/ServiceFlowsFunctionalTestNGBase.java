package com.latticeengines.serviceflows.functionalframework;

import java.util.Arrays;

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
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
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
    
    protected Table createTableFromDir(String tableName, String path, String lastModifiedColName) {
        Table table = new Table();
        table.setName(tableName);
        Extract extract = new Extract();
        extract.setName("e1");
        extract.setPath(path);
        table.addExtract(extract);
        PrimaryKey pk = new PrimaryKey();
        Attribute pkAttr = new Attribute();
        pkAttr.setName("Id");
        pk.setAttributes(Arrays.<Attribute>asList(new Attribute[] { pkAttr }));
        LastModifiedKey lmk = new LastModifiedKey();
        Attribute lastModifiedColumn = new Attribute();
        lastModifiedColumn.setName(lastModifiedColName);
        lmk.setAttributes(Arrays.<Attribute>asList(new Attribute[] { lastModifiedColumn }));
        table.setPrimaryKey(pk);
        table.setLastModifiedKey(lmk);
        return table;
    }
}
