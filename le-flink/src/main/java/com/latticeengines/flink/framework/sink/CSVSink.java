package com.latticeengines.flink.framework.sink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;

public class CSVSink extends FlinkSink {

    private final String recordDelimiter;
    private final String fieldDelimiter;

    public CSVSink(DataSet<?> dataSet, String pathWithFs, String recordDelimiter, String fieldDelimiter) {
        super(dataSet, pathWithFs, 1);
        this.recordDelimiter = recordDelimiter;
        this.fieldDelimiter = fieldDelimiter;
    }

    @SuppressWarnings("unchecked")
    public DataSink<?> createSink(DataSet<?> dataSet, String pathWithFs) {
        CsvOutputFormat outputFormat = new CsvOutputFormat(new Path(pathWithFs), recordDelimiter, fieldDelimiter);
        return dataSet.write(outputFormat, pathWithFs);
    }

}
