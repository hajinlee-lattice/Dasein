package com.latticeengines.dataflow.exposed.service;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.ExtractFilterBuilder;
import com.latticeengines.dataflow.functionalframework.DataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.ExtractFilter;
import com.latticeengines.domain.exposed.dataflow.TimestampExtractFilter;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import edu.emory.mathcs.backport.java.util.Collections;

public class ExtractFilterTestNG extends DataFlowFunctionalTestNGBase {

    public static final String OUTPUT_PATH = "/tmp/ExtractFilterTest/output";
    public static final String INPUT_PATH = "/tmp/ExtractFilterTest/input";
    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    private ExtractFilterBuilder extractFilterBuilder;

    private Configuration configuration = new Configuration();

    @BeforeClass(groups = "functional")
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        HdfsUtils.rmdir(configuration, INPUT_PATH);

        String extract1 = "com/latticeengines/dataflow/exposed/service/impl/extractFilterTest/ExtractFilterTest1.avro";
        String extract2 = "com/latticeengines/dataflow/exposed/service/impl/extractFilterTest/ExtractFilterTest2.avro";
        FileSystem fs = FileSystem.get(configuration);
        extractFilterBuilder.setLocal(false);
        List<AbstractMap.SimpleEntry<String, String>> copyEntries = new ArrayList<>();
        copyEntries.add(new AbstractMap.SimpleEntry(extract1, INPUT_PATH));
        copyEntries.add(new AbstractMap.SimpleEntry(extract2, INPUT_PATH));
        doCopy(fs, copyEntries);
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        HdfsUtils.rmdir(configuration, OUTPUT_PATH);
    }

    @Test(groups = "functional")
    public void testUnfiltered() throws Exception {
        Table table = getTable();

        Map<String, List<ExtractFilter>> extractFilters = new HashMap<>();

        execute(table, extractFilters);
        verifyNumRows(configuration, OUTPUT_PATH, 5218);
    }

    @Test(groups = "functional")
    @SuppressWarnings("unchecked")
    public void testFiltered() throws Exception {
        Table table = getTable();

        Map<String, List<ExtractFilter>> extractFilters = new HashMap<>();
        TimestampExtractFilter filter = new TimestampExtractFilter();
        filter.addDateRange(new DateTime(2016, 1, 1, 0, 1, DateTimeZone.UTC).getMillis(), //
                new DateTime(2016, 1, 31, 0, 1, DateTimeZone.UTC).getMillis());
        extractFilters.put("Account", Collections.singletonList(filter));

        execute(table, extractFilters);
        verifyNumRows(configuration, OUTPUT_PATH, 2609);
    }

    private Table getTable() {
        Table table = MetadataConverter.getTable(configuration, INPUT_PATH);
        table.getExtracts().clear();
        Extract extract1 = new Extract();
        extract1.setName("extract1");
        extract1.setPath(INPUT_PATH + "/ExtractFilterTest1.avro");
        table.getExtracts().add(extract1);

        Extract extract2 = new Extract();
        extract2.setName("extract2");
        extract2.setPath(INPUT_PATH + "/ExtractFilterTest2.avro");
        table.getExtracts().add(extract2);
        for (Extract extract : table.getExtracts()) {
            if (extract.getPath().contains("1")) {
                extract.setExtractionTimestamp(new DateTime(2016, 1, 1, 0, 1, DateTimeZone.UTC).getMillis());
            } else {
                extract.setExtractionTimestamp(new DateTime(2016, 2, 1, 0, 1, DateTimeZone.UTC).getMillis());
            }
        }
        return table;
    }

    private void execute(Table table, Map<String, List<ExtractFilter>> extractFilters) {
        DataFlowContext ctx = new DataFlowContext();
        Map<String, Table> sources = new HashMap<>();
        sources.put("ExtractFilterTest", table);

        ctx.setProperty(DataFlowProperty.SOURCETABLES, sources);
        ctx.setProperty(DataFlowProperty.CUSTOMER, "customer1");
        ctx.setProperty(DataFlowProperty.TARGETPATH, OUTPUT_PATH);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "ExtractFilterTest");
        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.FLOWNAME, "extractFilterBuilder");
        ctx.setProperty(DataFlowProperty.CHECKPOINT, true);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, configuration);
        ctx.setProperty(DataFlowProperty.ENGINE, "TEZ");
        ctx.setProperty(DataFlowProperty.EXTRACTFILTERS, extractFilters);

        Table output = dataTransformationService.executeNamedTransformation(ctx, "extractFilterBuilder");
    }
}
