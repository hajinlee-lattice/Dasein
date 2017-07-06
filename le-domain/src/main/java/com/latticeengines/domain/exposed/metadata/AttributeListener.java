package com.latticeengines.domain.exposed.metadata;

import java.util.Arrays;

import javax.persistence.PostLoad;

import org.apache.commons.lang3.StringUtils;

public class AttributeListener {

    @PostLoad
    public void tablePostLoad(Attribute attribute) {
        String enumValuesAsStr = attribute.getCleanedUpEnumValuesAsString();
        if (enumValuesAsStr != null) {
            attribute.setCleanedUpEnumValues(Arrays.<String>asList(StringUtils.split(enumValuesAsStr, ",")));
        }
    }
    
}
