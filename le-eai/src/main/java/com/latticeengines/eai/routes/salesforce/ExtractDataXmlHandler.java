package com.latticeengines.eai.routes.salesforce;

import java.io.File;
import java.util.Map;

import org.apache.camel.spring.SpringCamelContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.AvroContainer;

public class ExtractDataXmlHandler extends DefaultHandler {
    private static final Log log = LogFactory.getLog(ExtractDataXmlHandler.class);

    private Map<String, Attribute> tableAttributeMap;
    private String currentQname;
    private Attribute attr;
    private int processedRecords = 0;
    private int errorRecords = 0;
    private int totalRecords = 0;
    private AvroContainer avroContainer;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (qName.equals("queryResult")) {
            log.info("Thread id = " + Thread.currentThread().getName());
        } else if (qName.equals("records")) {
            avroContainer.newRecord();
            return;
        }

        attr = tableAttributeMap.get(qName);
        if (attr == null) {
            return;
        }
        String nilValue = attributes.getValue("xsi:nil");

        if (nilValue != null && nilValue.equals("true")) {
            avroContainer.setValueForAttribute(attr, null);
        }
        currentQname = qName;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (qName.equals("queryResult")) {
            avroContainer.endContainer();
            System.out.println("Total records = " + totalRecords);
            System.out.println("Successfully created records = " + processedRecords);
            System.out.println("Error records = " + errorRecords);
            return;
        } else if (qName.equals("records")) {
            totalRecords++;
            try {
                avroContainer.endRecord();
                processedRecords++;
            } catch (Exception e) {
                errorRecords++;
            }
            currentQname = null;
            return;
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
        avroContainer.setValueForAttribute(attr, new String(ch, start, length));
    }

    public String initialize(SpringCamelContext context, Table table) {
        this.avroContainer = new AvroContainer(context, table);
        this.tableAttributeMap = table.getNameAttributeMap();
        return avroContainer.getLocalAvroFile().getAbsolutePath();
    }

    public File getFile() {
        return avroContainer.getLocalAvroFile();
    }

}
