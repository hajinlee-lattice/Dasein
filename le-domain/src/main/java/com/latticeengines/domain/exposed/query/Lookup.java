package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
@Type(value = ColumnLookup.class, name = "columnLookup"), //
        @Type(value = ValueLookup.class, name = "valueLookup"), //
        @Type(value = RangeLookup.class, name = "rangeLookup") })
public abstract class Lookup {
}
