package com.latticeengines.flink.runtime;

import java.io.File;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.flink.framework.FlinkRuntimeTestNGBase;
import com.latticeengines.flink.framework.sink.AvroSink;
import com.latticeengines.flink.framework.sink.CSVSink;
import com.latticeengines.flink.framework.source.AvroSource;

public class SourceTestNG extends FlinkRuntimeTestNGBase {

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void test() throws Exception {
        FileUtils.deleteDirectory(new File(getInput()));
        FileUtils.forceMkdir(new File(getInput()));
        String outputDir = getOutputDir();

        prepareAvro(getInput() + File.separator + "1.avro");
        prepareAvro(getInput() + File.separator + "2.avro");

        DataSource<GenericRecord> dataSource = new AvroSource(env, getInput()).connect();
        DataSet<Tuple2<String, Integer>> data = dataSource.flatMap(new Tokenizer()).groupBy(0) //
                .aggregate(Aggregations.SUM, 1);

        // csv sink
        new CSVSink(data, outputDir + File.separator + "wiki.csv", "\n", ",").connect();

        // avro sink
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," + "\"fields\":["
                + "{\"name\":\"Word\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Count\",\"type\":[\"int\",\"null\"]}" + "]}");
        new AvroSink(data, outputDir + File.separator + "wiki", schema, 8).connect();

        execute();

        data.print();
    }

    private void prepareAvro(String input) {
        // avro sink
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," + "\"fields\":["
                + "{\"name\":\"Word\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Count\",\"type\":[\"int\",\"null\"]}" + "]}");
        Object[][] data = new Object[][] {
                {"a", 1}, //
                {"b", 2}, //
                {"c", 3}, //
                {"a", 1}, //
                {"b", 2}, //
                {"c", 3}, //
                {"a", 1}, //
                {"b", 2}, //
                {"c", 3}, //
        };
        List<GenericRecord> records = AvroUtils.convertToRecords(data, schema);
        try {
            AvroUtils.writeToLocalFile(schema, records, input);
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare input avro file", e);
        }
    }

    @Override
    protected String getTestName() {
        return "SourceTest";
    }

    private class Tokenizer implements FlatMapFunction<GenericRecord, Tuple2<String, Integer>> {

        @Override
        public void flatMap(GenericRecord record, Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<>(record.get("Word").toString(), (Integer) record.get("Count")));
        }
    }

}
