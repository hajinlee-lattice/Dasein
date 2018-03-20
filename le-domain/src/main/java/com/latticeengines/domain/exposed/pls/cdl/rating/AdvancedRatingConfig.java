package com.latticeengines.domain.exposed.pls.cdl.rating;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = CrossSellRatingConfig.class, name = "cross_sell"), //
        @Type(value = RuleBasedRatingConfig.class, name = "rule_based"), //
        @Type(value = CustomEventRatingConfig.class, name = "custom_event"), //
})
public interface AdvancedRatingConfig {
    void copyConfig(AdvancedRatingConfig config);
}
