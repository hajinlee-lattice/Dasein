package com.latticeengines.dataflow.exposed.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.functionalframework.DataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class DataTransformationServiceImplTestNG extends DataFlowFunctionalTestNGBase {

    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    private CascadingDataFlowBuilder sampleDataFlowBuilder;

    @Autowired
    private CascadingDataFlowBuilder tableWithExtractsDataFlowBuilder;

    @Resource(name = "yarnConfiguration")
    private Configuration config;

    private String lead;
    private String opportunity;
    private String contact;
    private String extract1;
    private String extract2;
    private String extract3;

    @BeforeMethod(groups = "functional")
    public void setup() throws Exception {
        if (sampleDataFlowBuilder.isLocal()) {
            config.set("fs.defaultFS", "file:///");
            config.set("fs.default.name", "file:///");
        }

        HdfsUtils.rmdir(config, "/tmp/EventTable");
        HdfsUtils.rmdir(config, "/tmp/CombinedImportTable");
        HdfsUtils.rmdir(config, "/tmp/checkpoints");
        HdfsUtils.rmdir(config, "/tmp/avro");

        lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        opportunity = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro")
                .getPath();
        contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro")
                .getPath();
        extract1 = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/file1").getPath()
                + "/*.avro";
        extract2 = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/file2").getPath()
                + "/*.avro";
        extract3 = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/file3").getPath()
                + "/*.avro";

        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();

        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + extract1, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + extract2, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + extract3, "/tmp/avro"));

        if (!sampleDataFlowBuilder.isLocal()) {
            lead = "/tmp/avro/Lead.avro";
            opportunity = "/tmp/avro/Opportunity.avro";
            FileSystem fs = FileSystem.get(config);
            doCopy(fs, entries);
        }

        if (!tableWithExtractsDataFlowBuilder.isLocal()) {
            extract1 = "/tmp/avro/file1.avro";
            extract2 = "/tmp/avro/file2.avro";
            extract3 = "/tmp/avro/file3.avro";
            FileSystem fs = FileSystem.get(config);
            doCopy(fs, entries);
        }
    }

    @Test(groups = "functional", dataProvider = "engineProvider", enabled = true)
    public void executeNamedTransformation(String engine) throws Exception {
        Map<String, Table> sources = new HashMap<>();
        sources.put("Lead", MetadataConverter.getTable(config, lead));
        sources.put("Opportunity", MetadataConverter.getTable(config, opportunity));
        sources.get("Lead").getAttribute("Email").setApprovedUsage(ApprovedUsage.MODEL);

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty(DataFlowProperty.SOURCETABLES, sources);
        ctx.setProperty(DataFlowProperty.CUSTOMER, "customer1");
        ctx.setProperty(DataFlowProperty.TARGETPATH, "/tmp/EventTable");
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "EventTable");
        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.FLOWNAME, "SampleDataFlow-Lead*Oppty");
        ctx.setProperty(DataFlowProperty.CHECKPOINT, true);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, config);
        ctx.setProperty(DataFlowProperty.ENGINE, engine);
        dataTransformationService.executeNamedTransformation(ctx, "sampleDataFlowBuilder");

        // verifyMetadata(table, "/tmp/EventTable");
        verifyNumRows(config, "/tmp/EventTable", 308);
    }

    @DataProvider(name = "engineProvider")
    public Object[][] getEngine() {
        return new Object[][] { { "TEZ" }, { "FLINK" } };
    }

    @Test(groups = "functional", dataProvider = "engineProvider", enabled = true)
    public void executeNamedTransformationForTableSource(String engine) throws Exception {
        Map<String, Table> sources = new HashMap<>();
        Table table = new Table();
        table.setName("source");
        Extract e1 = new Extract();
        e1.setName("e1");
        e1.setPath(extract1);
        Extract e2 = new Extract();
        e2.setName("e2");
        e2.setPath(extract2);
        Extract e3 = new Extract();
        e3.setName("e3");
        e3.setPath(extract3);
        table.addExtract(e1);
        table.addExtract(e2);
        table.addExtract(e3);
        PrimaryKey pk = new PrimaryKey();
        Attribute pkAttr = new Attribute();
        pkAttr.setName("ID");
        pk.addAttribute(pkAttr.getName());
        LastModifiedKey lk = new LastModifiedKey();
        Attribute lkAttr = new Attribute();
        lkAttr.setName("LastUpdatedDate");
        lk.addAttribute(lkAttr.getName());
        table.setPrimaryKey(pk);
        table.setLastModifiedKey(lk);
        table.addAttribute(pkAttr);
        table.addAttribute(lkAttr);

        sources.put("Source", table);

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty(DataFlowProperty.SOURCETABLES, sources);
        ctx.setProperty(DataFlowProperty.CUSTOMER, "customer2");
        ctx.setProperty(DataFlowProperty.TARGETPATH, "/tmp/CombinedImportTable");
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "CombinedImportTable");
        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.FLOWNAME, "TableWithExtractsDataFlow");
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, config);
        ctx.setProperty(DataFlowProperty.ENGINE, "TEZ");

        dataTransformationService.executeNamedTransformation(ctx, "tableWithExtractsDataFlowBuilder");
        verifyNumRows(config, "/tmp/CombinedImportTable", 7);
    }

    @Test(groups = "functional", dataProvider = "errorUseCaseProvider", //
            dependsOnMethods = { "executeNamedTransformationForTableSource" })
    public void executeNamedTransformationForErrors(Table table, String message) throws Exception {
        Map<String, Table> sourceTables = new HashMap<>();
        sourceTables.put("Source", table);

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty(DataFlowProperty.SOURCETABLES, sourceTables);
        ctx.setProperty(DataFlowProperty.CUSTOMER, "customer2");
        ctx.setProperty(DataFlowProperty.TARGETPATH, "/tmp/CombinedImportTable");
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "CombinedImportTable");
        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.FLOWNAME, "TableWithExtractsDataFlow");
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, config);
        ctx.setProperty(DataFlowProperty.ENGINE, "TEZ");

        boolean exception = false;
        try {
            dataTransformationService.executeNamedTransformation(ctx, "tableWithExtractsDataFlowBuilder");
        } catch (LedpException e) {
            exception = true;
            assertEquals(e.getMessage(), message);
        }
        assertTrue(exception);

    }

    @DataProvider(name = "errorUseCaseProvider")
    public Object[][] getErrorUseCaseProvider() {
        Table tableNoName = new Table();

        Table tableNoExtracts = new Table();
        tableNoExtracts.setName("tableNoExtract");

        Table tableExtractNoName = new Table();
        tableExtractNoName.setName("tableExtractNoName");
        Extract tableExtractNoNameExtract = new Extract();
        tableExtractNoName.addExtract(tableExtractNoNameExtract);

        Table tableExtractNoPath = new Table();
        tableExtractNoPath.setName("tableExtractNoPath");
        Extract tableExtractNoPathExtract = new Extract();
        tableExtractNoPathExtract.setName("extract1");
        tableExtractNoPath.addExtract(tableExtractNoPathExtract);

        Table tableExtractNoPKAttribute = new Table();
        tableExtractNoPKAttribute.setName("tableExtractNoPKAttribute");
        Extract tableExtractNoPKAttributeExtract = new Extract();
        tableExtractNoPKAttributeExtract.setName("extract1");
        tableExtractNoPKAttributeExtract.setPath("/extract1");
        tableExtractNoPKAttribute.addExtract(tableExtractNoPKAttributeExtract);
        PrimaryKey pk = new PrimaryKey();
        tableExtractNoPKAttribute.setPrimaryKey(pk);

        return new Object[][] { { tableNoName, "Table has no name." }, //
                { tableNoExtracts, "Table tableNoExtract has no extracts." }, //
                { tableExtractNoName, "Extract for table tableExtractNoName has no name." }, //
                { tableExtractNoPath, "Extract extract1 for table tableExtractNoPath has no path." }, //
                { tableExtractNoPKAttribute, "Primary key of table tableExtractNoPKAttribute has no attributes." } };
    }

}
