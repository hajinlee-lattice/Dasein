package com.latticeengines.flink.framework.sink;

import org.apache.avro.Schema;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.core.fs.Path;

public class AvroSink extends FlinkSink {

    private final Schema schema;

    public AvroSink(DataSet<?> dataSet, String pathWithFs, Schema schema, int numParallelism) {
        super(dataSet, pathWithFs, numParallelism);
        this.schema = schema;
    }

    @SuppressWarnings("unchecked")
    public DataSink<?> createSink(DataSet<?> dataSet, String pathWithFs) {
        AvroOutputFormat outputFormat = new AvroOutputFormat(new Path(pathWithFs), schema);
        return dataSet.write((FileOutputFormat) outputFormat, pathWithFs);
    }

}
