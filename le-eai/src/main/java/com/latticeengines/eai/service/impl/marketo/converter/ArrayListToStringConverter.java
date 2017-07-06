package com.latticeengines.eai.service.impl.marketo.converter;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.apache.camel.Converter;
import org.apache.commons.lang3.StringUtils;

@Converter
public class ArrayListToStringConverter {

    @Converter
    public String convertToInputStream(ArrayList<?> list) throws FileNotFoundException {
        return StringUtils.join(list, ",");
    }

}
