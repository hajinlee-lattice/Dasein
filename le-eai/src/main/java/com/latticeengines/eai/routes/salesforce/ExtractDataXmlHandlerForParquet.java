package com.latticeengines.eai.routes.salesforce;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import parquet.avro.AvroParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.impl.AvroTypeConverter;

public class ExtractDataXmlHandlerForParquet extends DefaultHandler {
    private static final Log log = LogFactory.getLog(ExtractDataXmlHandlerForParquet.class);

    private TypeConverterRegistry typeConverterRegistry;
    private Map<String, Attribute> tableAttributeMap;
    private Schema schema;
    private GenericRecord record;
    private String currentQname;
    private Attribute attr;
    private int processedRecords = 0;
    private int errorRecords = 0;
    private int totalRecords = 0;
    private File file;

    private AvroParquetWriter<GenericRecord> parquetWriter;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (qName.equals("queryResult")) {
            log.info("Thread id = " + Thread.currentThread().getName());
        } else if (qName.equals("records")) {
            record = new GenericData.Record(schema);
            return;
        }

        attr = tableAttributeMap.get(qName);
        if (attr == null) {
            return;
        }
        String nilValue = attributes.getValue("xsi:nil");

        if (nilValue != null && nilValue.equals("true")) {
            record.put(qName, AvroTypeConverter.getEmptyValue(Type.valueOf(attr.getPhysicalDataType())));
        }
        currentQname = qName;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        try {
            if (qName.equals("queryResult")) {
                parquetWriter.close();
                System.out.println("Total records = " + totalRecords);
                System.out.println("Successfully created records = " + processedRecords);
                System.out.println("Error records = " + errorRecords);
                return;
            } else if (qName.equals("records")) {
                totalRecords++;
                try {
                    parquetWriter.write(record);
                    processedRecords++;
                } catch (Exception e) {
                    errorRecords++;
                }
                currentQname = null;
                record = null;
                return;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Attribute attr = tableAttributeMap.get(qName);
        if (attr != null) {
            return;
        }

    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (currentQname == null) {
            return;
        }
        String value = new String(ch, start, length);
        Type type = Type.valueOf(attr.getPhysicalDataType());

        record.put(currentQname,
                AvroTypeConverter.convertIntoJavaValueForAvroType(typeConverterRegistry, type, attr, value));
    }

    public String initialize(TypeConverterRegistry typeConverterRegistry, Table table) {
        this.typeConverterRegistry = typeConverterRegistry;
        this.tableAttributeMap = table.getNameAttributeMap();
        this.schema = table.getSchema();

        String fileName = table.getName() + "_" + new SimpleDateFormat("dd-MM-yyyy").format(new Date()) + ".pqt";
        CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
        int blockSize = 256 * 1024 * 1024;
        int pageSize = 64 * 1024;
        Path outputPath = new Path("file://" + System.getProperty("user.dir") + "/" + fileName);
        try {
            parquetWriter = new AvroParquetWriter<>(outputPath, schema, compressionCodecName, blockSize, pageSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.file = new File(fileName);

        return fileName;
    }

    public File getFile() {
        return file;
    }
}
