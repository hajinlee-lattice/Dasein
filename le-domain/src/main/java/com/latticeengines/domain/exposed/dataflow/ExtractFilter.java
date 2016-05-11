package com.latticeengines.domain.exposed.dataflow;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.metadata.Extract;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
@JsonSubTypes.Type(value = TimestampExtractFilter.class) })
public abstract class ExtractFilter {
    public abstract boolean allows(Extract extract);
}
