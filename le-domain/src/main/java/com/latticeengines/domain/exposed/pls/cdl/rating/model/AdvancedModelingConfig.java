package com.latticeengines.domain.exposed.pls.cdl.rating.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = CrossSellModelingConfig.class, name = "cross_sell"), //
        @Type(value = ProspectingModelingConfig.class, name = "prospecting"), //
        @Type(value = CustomEventModelingConfig.class, name = "custom_event"), //
})
public interface AdvancedModelingConfig {
    void copyConfig(AdvancedModelingConfig config);

    String getDataCloudVersion();
}
