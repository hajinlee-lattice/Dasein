package com.latticeengines.eai.routes.salesforce;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.latticeengines.domain.exposed.eai.Table;

@Component("extractDataXmlHandler")
public class ExtractDataXmlHandler extends DefaultHandler {

    private Table table;
    private Schema schema;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        System.out.println("Start element");
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
    }

    public Table getTable() {
        return table;
    }

    public void initialize(Table table) {

    }
}
