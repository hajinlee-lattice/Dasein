package com.latticeengines.flink.job;

import java.io.File;
import java.util.Properties;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import com.latticeengines.flink.FlinkJobProperty;
import com.latticeengines.flink.framework.source.FlinkSource;
import com.latticeengines.flink.framework.source.TextSource;
import com.latticeengines.flink.runtime.Tokenizer;

public class WordCountJob extends FlinkJob {

    @Override
    protected void connect(ExecutionEnvironment env, Properties properties) {
        String inputText = properties.getProperty(FlinkJobProperty.INPUT);
        String outputDir = properties.getProperty(FlinkJobProperty.TARGET_PATH);
        String outputFile = outputDir + File.separator + "output.csv";
        FlinkSource source = new TextSource(env, inputText);

        // get input data
        @SuppressWarnings("unchecked")
        DataSource<String> text = (DataSource<String>) source.connect();
        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .aggregate(Aggregations.SUM, 1);
        counts.writeAsCsv(outputFile, "\n", ",")
                .setParallelism(1); // always generate a single csv
    }

}
