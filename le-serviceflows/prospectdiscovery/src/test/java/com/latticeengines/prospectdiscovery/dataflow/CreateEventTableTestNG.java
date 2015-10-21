package com.latticeengines.prospectdiscovery.dataflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class CreateEventTableTestNG extends ServiceFlowsFunctionalTestNGBase {

    private String account;
    private String opportunity;
    private String contact;
    private String stoplist;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/TmpEventTable");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/checkpoints");
        account = ClassLoader.getSystemResource("Account").getPath() + "/*.avro";
        opportunity = ClassLoader.getSystemResource("Opportunity").getPath() + "/*.avro";
        contact = ClassLoader.getSystemResource("Contact").getPath() + "/*.avro";
        stoplist = ClassLoader.getSystemResource("Stoplist").getPath() + "/*.avro";
    }

    @Test(groups = "functional")
    public void executeDataFlow() throws Exception {
        Map<String, Table> sources = new HashMap<>();
        sources.put("Account", //
                MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, account, "Id", "CreatedDate"));
        sources.put("Opportunity", //
                MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, opportunity, "Id", "LastModifiedDate"));
        sources.put("Contact", //
                MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, contact, "Id", "LastModifiedDate"));
        sources.put("Stoplist", //
                MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, stoplist, null, null));

        DataFlowContext ctx = super.createDataFlowContext();
        ctx.setProperty("SOURCETABLES", sources);
        ctx.setProperty("CUSTOMER", "customer1");
        ctx.setProperty("TARGETPATH", "/tmp/TmpEventTable");
        ctx.setProperty("TARGETTABLENAME", "TmpEventTable");
        ctx.setProperty("FLOWNAME", "CreateEventTable");
        ctx.setProperty("CHECKPOINT", true);

        Table result = super.executeDataFlow(ctx, "createEventTable");

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> outputTable = readTable(result.getExtracts().get(0).getPath() + "/*.avro");
        List<GenericRecord> accountTable = readTable(account);

        Assert.assertTrue(identicalSets(outputTable, "Id", accountTable, "Id"));

        // Confirm that email stop-list filtering worked
        for (GenericRecord record : outputTable) {
            Assert.assertTrue(record.get("Domain") == null || !record.equals("gmail.com"));
        }
    }
}
