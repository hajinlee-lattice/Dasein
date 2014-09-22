package com.latticeengines.eai.routes.salesforce;

import java.util.Map;

import org.apache.avro.Schema;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;

public class ExtractDataXmlHandler extends DefaultHandler {

    private Map<String, Attribute> table;
    private Schema schema;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        System.out.println("Start element " + qName);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        System.out.println("End element " + qName);
    }

    public void initialize(Table table) {
        this.table = table.getNameAttributeMap();
    }

}
