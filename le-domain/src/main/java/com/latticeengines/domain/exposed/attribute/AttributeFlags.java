package com.latticeengines.domain.exposed.attribute;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
@JsonSubTypes.Type(value = CompanyProfileAttributeFlags.class), //
        @JsonSubTypes.Type(value = EnrichmentAttributeFlags.class) })
public abstract class AttributeFlags {
}
