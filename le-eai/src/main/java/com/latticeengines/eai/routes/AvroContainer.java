package com.latticeengines.eai.routes;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.Deflater;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.camel.spring.SpringCamelContext;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.service.impl.AvroTypeConverter;

public class AvroContainer {

    private Table table;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private File file;
    private GenericRecord record;
    private Schema schema;
    private TypeConverterRegistry typeConverterRegistry;

    public AvroContainer(SpringCamelContext context, Table table) {
        this.typeConverterRegistry = context.getTypeConverterRegistry();
        this.table = table;
        this.schema = table.getSchema();
        if (schema == null) {
            throw new RuntimeException("Schema cannot be null.");
        }
        this.file = new File(String.format("%s-%s.avro", table.getName(),
                new SimpleDateFormat("dd-MM-yyyy").format(new Date())));

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        try {
            dataFileWriter.setCodec(CodecFactory.deflateCodec(Deflater.BEST_COMPRESSION));
            dataFileWriter.create(schema, file);
            return;
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
                dataFileWriter.append(record);
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

    public File getLocalAvroFile() {
        return file;
    }
}
