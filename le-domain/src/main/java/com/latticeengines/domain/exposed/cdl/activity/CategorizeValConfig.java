package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CategorizeDoubleConfig.class, name = CategorizeDoubleConfig.NAME)
})
public abstract class CategorizeValConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    protected static final String CATEGORY_UNDEFINED = "undefined";

    @JsonIgnore
    public abstract Set<String> getCategoryNames();

}
