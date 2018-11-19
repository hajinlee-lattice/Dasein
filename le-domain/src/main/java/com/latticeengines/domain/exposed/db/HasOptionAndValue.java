package com.latticeengines.domain.exposed.db;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.latticeengines.domain.exposed.metadata.DataCollectionProperty;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ @Type(value = DataCollectionProperty.class, name = "dataCollectionProperty"), })
public interface HasOptionAndValue {
    String getOption();

    void setOption(String option);

    String getValue();

    void setValue(String value);
}
