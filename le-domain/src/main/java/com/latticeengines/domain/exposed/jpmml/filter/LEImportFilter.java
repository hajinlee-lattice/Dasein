package com.latticeengines.domain.exposed.jpmml.filter;

import org.jpmml.model.ImportFilter;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;

public class LEImportFilter extends ImportFilter {
    
    private StringBuffer originalVersion = new StringBuffer();

    public LEImportFilter(XMLReader reader) {
        super(reader);
    }

    @Override
    public void startElement(String namespaceURI, String localName, String qualifiedName, Attributes attributes)
            throws SAXException {

        if (("PMML").equals(localName)) {
            int index = attributes.getIndex("", localName);
            AttributesImpl result = new AttributesImpl(attributes);
            if(index < 0){
                originalVersion.append(result.getValue(0));
            } else {
                originalVersion.append(result.getValue(index));
            }
        }

        super.startElement(namespaceURI, localName, qualifiedName, attributes);
    }
    
    public String getOriginalVersion() {
        return originalVersion.toString();
    }
}
