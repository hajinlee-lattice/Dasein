package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=As.WRAPPER_OBJECT, property="property")
@JsonSubTypes({
    @Type(value=ProspectDiscoveryOption.class, name="prospectDiscoveryOption")
})
public interface HasOptionAndValue {
    String getOption();

    void setOption(String option);

    String getValue();

    void setValue(String value);
}
