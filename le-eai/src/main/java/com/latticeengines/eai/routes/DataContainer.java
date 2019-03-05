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
import org.apache.camel.spring.SpringCamelContext;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.ValueConverter;
import com.latticeengines.eai.service.impl.AvroTypeConverter;
import com.latticeengines.eai.service.impl.CamelValueConverter;

public class DataContainer {

    private Table table;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private File file;
    private GenericRecord record;
    private Schema schema;
    private ValueConverter valueConverter;

    public DataContainer(SpringCamelContext context, Table table) {
        ValueConverter valueConverter = new CamelValueConverter(context.getTypeConverterRegistry());
        initialize(valueConverter, table, false);
    }

    public DataContainer(ValueConverter valueConverter, Table table) {
        initialize(valueConverter, table, false);
    }

    public DataContainer(ValueConverter valueConverter, Table table, boolean uniqueName) {
        initialize(valueConverter, table, uniqueName);
    }

    private void initialize(ValueConverter valueConverter, Table table, boolean uniquedName) {
        this.valueConverter = valueConverter;
        this.table = table;
        this.schema = table.getSchema();
        if (schema == null) {
            throw new RuntimeException("Schema cannot be null.");
        }
        if (uniquedName) {
            this.file = new File(String.format("%s.avro", NamingUtils.uuid(table.getName())));
        } else {
            this.file = new File(
                    String.format("%s-%s.avro", table.getName(), new SimpleDateFormat("yyyy-MM-dd").format(new Date())));
        }

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
            if(valueConverter == null || valueConverter.autoFillNullValue()) {
                record.put(attribute.getName(),
                        AvroTypeConverter.getEmptyValue(Type.valueOf(attribute.getPhysicalDataType())));
            }
        } else {
            try {
                Type type = Type.valueOf(attribute.getPhysicalDataType());
                record.put(attribute.getName(), AvroTypeConverter.convertIntoJavaValueForAvroType(valueConverter,
                        type, attribute, value));
            } catch (Exception e) {
                throw new RuntimeException(String.format("Cannot convert value %s to Type %s for attribute %s",
                        String.valueOf(value), attribute.getPhysicalDataType(), attribute.getName()));
            }
        }
    }

    public Object getValueForAttribute(Attribute attribute) {
        return record.get(attribute.getName());
    }

    public File getLocalDataFile() {
        return file;
    }

    public void flush() {
        try {
            dataFileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
