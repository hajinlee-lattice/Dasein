//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.latticeengines.flink.framework.sink;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.Path;

public class AvroOutputFormat extends FileOutputFormat<Tuple> {
    private static final long serialVersionUID = 1L;
    private final String serializedSchema;
    private Schema schema;
    private transient DataFileWriter<GenericRecord> dataFileWriter;

    AvroOutputFormat(Path filePath, Schema schema) {
        super(filePath);
        this.serializedSchema = schema.toString();
    }

    protected String getDirectoryFileName(int taskNumber) {
        return super.getDirectoryFileName(taskNumber) + ".avro";
    }

    public void writeRecord(Tuple record) throws IOException {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        int i = 0;
        for (Schema.Field field: schema.getFields()) {
            builder.set(field, record.getField(i));
            i++;
        }
        this.dataFileWriter.append(builder.build());
    }

    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
        schema = new Schema.Parser().parse(serializedSchema);
        this.dataFileWriter = writer.create(schema, this.stream);

    }

    public void close() throws IOException {
        this.dataFileWriter.flush();
        this.dataFileWriter.close();
        super.close();
    }
}
