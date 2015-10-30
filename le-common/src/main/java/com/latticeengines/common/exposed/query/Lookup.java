package com.latticeengines.common.exposed.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=As.WRAPPER_OBJECT, property="property")
@JsonSubTypes({
    @Type(value=MathematicalLookup.class, name="mathematicalLookup"),
    @Type(value=SingleReferenceLookup.class, name="singleReferenceLookup"),
})
public abstract class Lookup {
}
