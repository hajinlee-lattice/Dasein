package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class CdlPivotScoreAndEventTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CdlPivotScoreAndEventTestNG.class);

    private PivotScoreAndEventParameters getStandardParameters() {
        PivotScoreAndEventParameters params = new PivotScoreAndEventParameters("InputTable");
        String modelguid1 = "ms__71fa73d2-ce4b-483a-ab1a-02e4471cd0fc-RatingEn";
        String modelguid2 = "ms__af81bb1f-a71e-4b3a-89cd-3a9d0c02b0d1-CDLEnd2E";
        params.setAvgScores(ImmutableMap.of(modelguid1, 0.05, modelguid2, 0.05));
        params.setExpectedValues(ImmutableMap.of(modelguid1, true, modelguid2, false));
        return params;
    }

    @Override
    protected Map<String, Table> getSources() {
        URL url = ClassLoader.getSystemResource("pivotScoreAndEvent/CDLScoreOutput/InputTable");
        Configuration config = new Configuration();
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        Table t = MetadataConverter.getTable(config, url.getPath() + "/score_data.avro");
        Extract e = new Extract();
        e.setName("ScoreResult");
        e.setPath(url.getPath());
        t.setExtracts(Arrays.asList(e));
        return ImmutableMap.of("InputTable", t);
    }

    @Override
    protected String getFlowBeanName() {
        return "pivotScoreAndEvent";
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        executeDataFlow(getStandardParameters());
        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), 69);
    }

    public static void main(String[] args) throws IOException {
        String uuid = "f7f1eb16-0d26-4aa1-8c4a-3ac696e13d06";
        URL url1 = ClassLoader
                .getSystemResource("pivotScoreAndEvent/CDLScoreOutput/af81bb1f-a71e-4b3a-89cd-3a9d0c02b0d1-0.avro");

        DatumWriter userDatumWriter = new GenericDatumWriter<GenericRecord>();
        DataFileWriter dataFileWriter = new DataFileWriter(userDatumWriter);
        Configuration config = new Configuration();
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);

        Table t = MetadataConverter.getTable(config, url1.getPath());
        System.out.println(AvroUtils.readSchemaFromLocalFile(url1.getPath()));
        // Attribute a = new Attribute();
        // a.setName(ScoreResultField.ModelId.displayName);
        // a.setDisplayName(a.getName());
        // a.setPhysicalDataType("String");
        // t.addAttribute(a);
        t.getAttribute(InterfaceName.__Composite_Key__.name()).setName(InterfaceName.AnalyticPurchaseState_ID.name());
        t.getAttribute(InterfaceName.AnalyticPurchaseState_ID.name()).setPhysicalDataType("long");
        t.getAttribute(InterfaceName.AnalyticPurchaseState_ID.name())
                .setDisplayName(InterfaceName.AnalyticPurchaseState_ID.name());

        Schema s = TableUtils.createSchema("ScoreResult", t);
        System.out.println(s);
        dataFileWriter.create(s, new File("score_data.avro"));
        List<GenericRecord> records = AvroUtils.readFromLocalFile(url1.getPath());
        GenericRecordBuilder builder = new GenericRecordBuilder(s);
        long id = 2;
        for (GenericRecord r : records) {
            // builder.set(ScoreResultField.ModelId.displayName, "ms__" + uuid +
            // "-PLS_model");
            for (Field f : s.getFields()) {

                if (!f.name().equals(InterfaceName.AnalyticPurchaseState_ID.name())) {
                    builder.set(f, r.get(f.name()));
                } else {
                    builder.set(InterfaceName.AnalyticPurchaseState_ID.name(), id++);
                }
            }
            dataFileWriter.append(builder.build());
        }
        dataFileWriter.close();
    }
}
