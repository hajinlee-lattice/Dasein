package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;

import java.io.InputStreamReader;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class PivotScoreAndEventTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PivotScoreAndEventTestNG.class);

    private PivotScoreAndEventParameters getStandardParameters() {
        PivotScoreAndEventParameters params = new PivotScoreAndEventParameters("ScoreOutput");
        String modelguid = "ms__f7f1eb16-0d26-4aa1-8c4a-3ac696e13d06-PLS_model";
        params.setAvgScores(ImmutableMap.of(modelguid, 0.05));
        params.setExpectedValues(ImmutableMap.of(modelguid, false));
        return params;
    }

    @Override
    protected String getFlowBeanName() {
        return "pivotScoreAndEvent";
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        executeDataFlow(getStandardParameters());
        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), 17);

        CSVFormat format = LECSVFormat.format.withSkipHeaderRecord(true);
        try (CSVParser parser = new CSVParser(new InputStreamReader(
                ClassLoader.getSystemResourceAsStream(String.format("%s/%s/pivot_score_event_result.csv", //
                        getFlowBeanName(), getStandardParameters().getScoreOutputTableName()))),
                format)) {
            List<CSVRecord> csvRecords = parser.getRecords();

            for (int i = 0; i < outputRecords.size(); i++) {
                CSVRecord csvRecord = csvRecords.get(i);
                GenericRecord avroRecord = outputRecords.get(i);
                log.info("avro:" + avroRecord);
                log.info("csv:" + csvRecord);
                for (int j = 0; j < csvRecord.size(); j++) {
                    assertEquals(Double.valueOf(csvRecord.get(j)) - Double.valueOf(avroRecord.get(j).toString()), 0.0);
                }
            }
        }
    }

    // public static void main(String[] args) throws IOException {
    // String uuid = "f7f1eb16-0d26-4aa1-8c4a-3ac696e13d06";
    // URL url1 =
    // ClassLoader.getSystemResource("pivotScoreAndEvent/ScoreOutput/score_data.avro");
    //
    // DatumWriter userDatumWriter = new GenericDatumWriter<GenericRecord>();
    // DataFileWriter dataFileWriter = new DataFileWriter(userDatumWriter);
    // Configuration config = new Configuration();
    // config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    //
    // Table t = MetadataConverter.getTable(config, url1.getPath());
    // Attribute a = new Attribute();
    // a.setName(ScoreResultField.ModelId.displayName);
    // a.setDisplayName(a.getName());
    // a.setPhysicalDataType("String");
    // t.addAttribute(a);
    //
    // Schema s = TableUtils.createSchema("scoringtable", t);
    // dataFileWriter.create(s, new File("score_data.avro"));
    // List<GenericRecord> records =
    // AvroUtils.readFromLocalFile(url1.getPath());
    // GenericRecordBuilder builder = new GenericRecordBuilder(s);
    // for (GenericRecord r : records) {
    // builder.set(ScoreResultField.ModelId.displayName, "ms__" + uuid +
    // "-PLS_model");
    // for (Field f : s.getFields()) {
    // if (!f.name().equals(ScoreResultField.ModelId.displayName)) {
    // builder.set(f, r.get(f.name()));
    // }
    // }
    // dataFileWriter.append(builder.build());
    // }
    // dataFileWriter.close();
    // }
}
