package com.latticeengines.flink.runtime;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.testng.annotations.Test;

import com.latticeengines.flink.framework.FlinkRuntimeTestNGBase;
import com.latticeengines.flink.framework.sink.AvroSink;
import com.latticeengines.flink.framework.sink.CSVSink;
import com.latticeengines.flink.framework.source.TextSource;

public class TokenizerTestNG extends FlinkRuntimeTestNGBase {

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void test() {
        String input = getResourcePath("wiki.txt");
        String outputDir = getOutputDir();

        DataSource<String> text = (DataSource<String>) new TextSource(env, input).connect();
        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field
                        // "1"
                        .groupBy(0) //
                        .aggregate(Aggregations.SUM, 1);
        // csv sink
        new CSVSink(counts, outputDir + File.separator + "wiki.csv", "\n", ",").connect();

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," + "\"fields\":["
                + "{\"name\":\"Word\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Count\",\"type\":[\"int\",\"null\"]}" + "]}");
        new AvroSink(counts, outputDir + File.separator + "wiki", schema, 8).connect();
        execute();
    }

    @Override
    protected String getOperatorName() {
        return "Tokenizer";
    }

}
