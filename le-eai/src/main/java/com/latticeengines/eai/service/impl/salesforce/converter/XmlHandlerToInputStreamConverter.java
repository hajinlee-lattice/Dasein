package com.latticeengines.eai.service.impl.salesforce.converter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.apache.camel.Converter;

import com.latticeengines.eai.routes.salesforce.ExtractDataXmlHandler;

@Converter
public class XmlHandlerToInputStreamConverter {

    @Converter
    public InputStream convertToInputStream(ExtractDataXmlHandler handler) throws FileNotFoundException {
        return new FileInputStream(handler.getFile());
    }
}
