package com.latticeengines.eai.routes;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.camel.spring.SpringCamelContext;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.impl.AvroTypeConverter;

public class DataContainer {

    private Table table;
    private AvroParquetWriter<GenericRecord> dataFileWriter;
    private File file;
    private GenericRecord record;
    private Schema schema;
    private TypeConverterRegistry typeConverterRegistry;

    public DataContainer(SpringCamelContext context, Table table) {
        this.typeConverterRegistry = context.getTypeConverterRegistry();
        this.table = table;
        this.schema = table.getSchema();
        if (schema == null) {
            throw new RuntimeException("Schema cannot be null.");
        }
        this.file = new File(String.format("%s-%s.parquet", table.getName(),
                new SimpleDateFormat("dd-MM-yyyy").format(new Date())));
        CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
        int blockSize = 256 * 1024 * 1024;
        int pageSize = 64 * 1024;
        Path outputPath = new Path("file://" + System.getProperty("user.dir") + "/" + file.getName());
        try {
            dataFileWriter = new AvroParquetWriter<>(outputPath, schema, compressionCodecName, blockSize, pageSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Table getTable() {
        return table;
    }

    public void newRecord() {
        record = new GenericData.Record(schema);
    }

    public void endRecord() {
        if (record != null) {
            try {
                dataFileWriter.write(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void endContainer() {
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setValueForAttribute(Attribute attribute, Object value) {
        if (value == null) {
            record.put(attribute.getName(),
                    AvroTypeConverter.getEmptyValue(Type.valueOf(attribute.getPhysicalDataType())));
        } else {
            Type type = Type.valueOf(attribute.getPhysicalDataType());
            record.put(attribute.getName(),
                    AvroTypeConverter.convertIntoJavaValueForAvroType(typeConverterRegistry, type, attribute, value));
        }
    }

    public File getLocalDataFile() {
        return file;
    }
}
