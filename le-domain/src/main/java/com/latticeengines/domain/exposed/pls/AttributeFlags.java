package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({ 
        @JsonSubTypes.Type(value = CompanyProfileAttributeFlags.class, name = "companyProfileAttributeFlags"),
        @JsonSubTypes.Type(value = EnrichmentAttributeFlags.class, name = "enrichmentAttributeFlags") })
public abstract class AttributeFlags {
}
