package com.latticeengines.domain.exposed.metadata.datafeed.validator;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = AttributeLengthValidator.class, name = "attributeLengthValidator"),
        @JsonSubTypes.Type(value = SimpleValueFilter.class, name = "simpleValueFilter")
})
public abstract class TemplateValidator {
    /**
     *
     * @param record: (AttributeName -> String value from CSV) value pairs of a csv row.
     * @return true means accept else skip.
     */
    public abstract boolean accept(Map<String, String> record, Map<String, String> errorMsg);

}
