package com.latticeengines.dataplatform.exposed.sqoop.runtime.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.sqoop.mapreduce.AvroImportMapper;
import org.apache.sqoop.mapreduce.AvroJob;
import org.apache.sqoop.mapreduce.ImportJobBase;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.mortbay.log.Log;

import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.sqoop.runtime.mapreduce.LedpRecordImportCounter.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

/**
 * Imports records by transforming them to Avro records in an Avro data file.
 */
public class LedpCSVToAvroImportMapper extends AvroImportMapper {
    private final AvroWrapper<GenericRecord> wrapper = new AvroWrapper<GenericRecord>();
    private Schema schema;
    private Table table;
    private LargeObjectLoader lobLoader;
    private boolean bigDecimalFormatString;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        schema = AvroJob.getMapOutputSchema(conf);
        table = JsonUtils.deserialize(context.getConfiguration().get("lattice.eai.file.schema"), Table.class);
        lobLoader = new LargeObjectLoader(conf, FileOutputFormat.getWorkOutputPath(context));
        bigDecimalFormatString = conf.getBoolean(ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
                ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
    }

    @Override
    protected void map(LongWritable key, SqoopRecord val, Context context) throws IOException, InterruptedException {
        try {
            Log.info("Using LedpCSVToAvroImportMapper");
            Log.info("Table is: " + table);
            // Loading of LOBs was delayed until we have a Context.
            val.loadLargeObjects(lobLoader);
        } catch (SQLException sqlE) {
            throw new IOException(sqlE);
        }
        try {
            wrapper.datum(toGenericRecord(val));
            context.write(wrapper, NullWritable.get());
            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).increment(1);
        } catch (Exception e) {
            context.getCounter(RecordImportCounter.IGNORED_RECORDS).increment(1);
        }
    }

    private GenericRecord toGenericRecord(SqoopRecord val) {
        Map<String, Object> fieldMap = val.getFieldMap();
        GenericRecord record = new GenericData.Record(schema);
        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            record.put(entry.getKey(),toAvro2(String.valueOf(entry.getValue()), schema.getField(entry.getKey()), table
                            .getNameAttributeMap().get(entry.getKey())));
        }
        return record;
    }

    private Object toAvro2(String s, Field field, Attribute attr) {
        Type avroType = field.schema().getTypes().get(0).getType();
        switch (avroType) {
        case DOUBLE:
            return Double.valueOf(s);
        case FLOAT:
            return Float.valueOf(s);
        case INT:
            return Integer.valueOf(s);
        case LONG:
            // DateTimeFormatter dtf = null;
            DateFormat df = null;
            if (attr.getLogicalDataType().equals("date") || attr.getLogicalDataType().equals("Date")) {
                // dtf = ISODateTimeFormat.dateElementParser();
                df = new SimpleDateFormat("MM-dd-yyyy");

            } else if (attr.getLogicalDataType().equals("datetime") || attr.getLogicalDataType().equals("Timestamp")) {
                // dtf = ISODateTimeFormat.dateTimeParser();
            }
            try {
                Log.info(s);
                Log.info("parse :" + df.parse(s));
                return df.parse(s).getTime();
                // return dateTime.getMillis();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        case STRING:
            return s;
        case BOOLEAN:
            return Boolean.valueOf(s);
        case ENUM:
            return s;
        default:
            throw new RuntimeException("Not supported Field, avroType:" + avroType + ", logicalType:"
                    + attr.getLogicalDataType());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException {
        if (null != lobLoader) {
            lobLoader.close();
        }
        if(context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue() == 0){
            context.getCounter(RecordImportCounter.IGNORED_RECORDS).setValue(0);
        }
    }

    /**
     * Convert the Avro representation of a Java type (that has already been
     * converted from the SQL equivalent).
     * 
     * @param o
     * @return
     */
    private Object toAvro(Object o) {
        if (o instanceof BigDecimal) {
            if (bigDecimalFormatString) {
                return ((BigDecimal) o).toPlainString();
            } else {
                return o.toString();
            }
        } else if (o instanceof Date) {
            return ((Date) o).getTime();
        } else if (o instanceof Time) {
            return ((Time) o).getTime();
        } else if (o instanceof Timestamp) {
            return ((Timestamp) o).getTime();
        } else if (o instanceof BytesWritable) {
            BytesWritable bw = (BytesWritable) o;
            return ByteBuffer.wrap(bw.getBytes(), 0, bw.getLength());
        } else if (o instanceof BlobRef) {
            BlobRef br = (BlobRef) o;
            // If blob data is stored in an external .lob file, save the ref
            // file
            // as Avro bytes. If materialized inline, save blob data as Avro
            // bytes.
            byte[] bytes = br.isExternal() ? br.toString().getBytes() : br.getData();
            return ByteBuffer.wrap(bytes);
        } else if (o instanceof ClobRef) {
            throw new UnsupportedOperationException("ClobRef not suported");
        }
        // primitive types (Integer, etc) are left unchanged
        return o;
    }

    public static void main(String[] args) throws ParseException {
        String s = "10-20-2014";
        DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
        System.out.println(df.parse(s));

    }
}
