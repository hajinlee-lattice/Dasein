package com.latticeengines.domain.exposed.datacloud.check;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "CheckCode")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DuplicatedValueCheckParam.class, name = "DuplicatedValue"),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class CheckParam {

    @JsonProperty("CheckCode")
    private CheckCode checkCode;

    public CheckCode getCheckCode() {
        return checkCode;
    }

    protected void setCheckCode(CheckCode checkCode) {
        this.checkCode = checkCode;
    }
}
