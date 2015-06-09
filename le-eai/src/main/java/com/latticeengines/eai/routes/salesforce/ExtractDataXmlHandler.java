package com.latticeengines.eai.routes.salesforce;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.service.impl.AvroTypeConverter;

public class ExtractDataXmlHandler extends DefaultHandler {
    private static final Log log = LogFactory.getLog(ExtractDataXmlHandler.class);

    private TypeConverterRegistry typeConverterRegistry;
    private Map<String, Attribute> tableAttributeMap;
    private Schema schema;
    private GenericRecord record;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private String currentQname;
    private Attribute attr;
    private int processedRecords = 0;
    private int errorRecords = 0;
    private int totalRecords = 0;
    private File file;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (qName.equals("queryResult")) {
            log.info("Thread id = " + Thread.currentThread().getName());

            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            try {
                dataFileWriter.setCodec(CodecFactory.snappyCodec());
                dataFileWriter.create(schema, file);
                return;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
                dataFileWriter.close();
                System.out.println("Total records = " + totalRecords);
                System.out.println("Successfully created records = " + processedRecords);
                System.out.println("Error records = " + errorRecords);
                return;
            } else if (qName.equals("records")) {
                totalRecords++;
                try {
                    dataFileWriter.append(record);
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
        String fileName = table.getName() + "_" + new SimpleDateFormat("dd-MM-yyyy").format(new Date()) + ".avro";
        this.file = new File(fileName);
        return fileName;
    }

    public File getFile() {
        return file;
    }

}
