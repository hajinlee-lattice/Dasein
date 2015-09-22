package com.latticeengines.prospectdiscovery.dataflow;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class CreateEventTableTestNG extends ServiceFlowsFunctionalTestNGBase {
    
    private String account;
    private String opportunity;
    private String contact;
    
    @Autowired
    private CascadingDataFlowBuilder createEventTable;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        createEventTable.setLocal(true);
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/TmpEventTable");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/checkpoints");
        account = ClassLoader.getSystemResource("Account").getPath() + "/*.avro";
        opportunity = ClassLoader.getSystemResource("Opportunity").getPath() + "/*.avro";
        contact = ClassLoader.getSystemResource("Contact").getPath() + "/*.avro";
    }
    
    @Test(groups = "functional")
    public void executeDataFlow() throws Exception {
        Map<String, String> sources = new HashMap<>();
        sources.put("Account", account);
        sources.put("Opportunity", opportunity);
        sources.put("Contact", contact);
        
        DataFlowContext ctx = super.createDataFlowContext();
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("CUSTOMER", "customer1");
        ctx.setProperty("TARGETPATH", "/tmp/TmpEventTable");
        ctx.setProperty("FLOWNAME", "CreateEventTable");
        
        super.executeDataFlow(ctx, "createEventTable");
    }
}
