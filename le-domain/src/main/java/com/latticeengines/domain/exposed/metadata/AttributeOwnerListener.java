package com.latticeengines.domain.exposed.metadata;

import java.util.Arrays;

import javax.persistence.PostLoad;
import javax.persistence.PrePersist;

import org.apache.commons.lang3.StringUtils;

public class AttributeOwnerListener {

    @PostLoad
    public void tablePostLoad(AttributeOwner attributeOwner) {
        String attributesAsString = attributeOwner.getAttributesAsStr();
        if (attributesAsString != null) {
            attributeOwner.setAttributes(Arrays.asList(attributesAsString.split(",")));
        }
    }
    
    @PrePersist
    public void tablePrePersist(AttributeOwner attributeOwner) {
        attributeOwner.setAttributesAsStr(StringUtils.join(attributeOwner.getAttributes(), ","));
    }
    
}
