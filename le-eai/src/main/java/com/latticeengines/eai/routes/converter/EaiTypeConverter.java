package com.latticeengines.eai.routes.converter;

import java.io.InputStream;

import org.apache.camel.Converter;
import org.xml.sax.InputSource;

@Converter
public class EaiTypeConverter {

    @Converter
    public InputSource createInputSource(InputStream in) {
        return new InputSource(in);
    }

}
