package com.latticeengines.eai.routes.salesforce;

import java.io.File;
import java.util.Map;

import org.apache.camel.spring.SpringCamelContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.routes.DataContainer;

public class ExtractDataXmlHandler extends DefaultHandler {
    private static final Log log = LogFactory.getLog(ExtractDataXmlHandler.class);

    private Map<String, Attribute> tableAttributeMap;
    private String currentQname;
    private Attribute attr;
    private int processedRecords = 0;
    private int errorRecords = 0;
    private int totalRecords = 0;
    private DataContainer dataContainer;
    private String lmk;
    private long lmkValue;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (qName.equals("queryResult")) {
            log.info("Thread id = " + Thread.currentThread().getName());
        } else if (qName.equals("records")) {
            dataContainer.newRecord();
            return;
        }

        attr = tableAttributeMap.get(qName);
        if (attr == null) {
            return;
        }
        String nilValue = attributes.getValue("xsi:nil");

        if (nilValue != null && nilValue.equals("true")) {
            dataContainer.setValueForAttribute(attr, null);
        }
        currentQname = qName;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (qName.equals("queryResult")) {
            dataContainer.endContainer();
            System.out.println("Total records = " + totalRecords);
            System.out.println("Successfully created records = " + processedRecords);
            System.out.println("Error records = " + errorRecords);
            return;
        } else if (qName.equals("records")) {
            totalRecords++;
            try {
                dataContainer.endRecord();
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

        dataContainer.setValueForAttribute(attr, new String(ch, start, length));
        if (processedRecords == 0 && attr.getName().equals(lmk)) {
            String value = dataContainer.getValueForAttribute(attr).toString();
            lmkValue = Long.parseLong(value);
        }
    }

    public Long getLmkValue() {
        return lmkValue;
    }

    public String initialize(SpringCamelContext context, Table table) {
        this.dataContainer = new DataContainer(context, table);
        this.tableAttributeMap = table.getNameAttributeMap();
        this.lmk = "LastModifiedDate";
        return dataContainer.getLocalDataFile().getName();
    }

    public File getFile() {
        return dataContainer.getLocalDataFile();
    }

}
