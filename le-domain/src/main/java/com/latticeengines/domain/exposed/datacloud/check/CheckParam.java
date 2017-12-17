package com.latticeengines.domain.exposed.datacloud.check;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "CheckCode")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DuplicatedValueCheckParam.class, name = "DuplicatedValue"),
        @JsonSubTypes.Type(value = ExceededCountCheckParam.class, name = "ExceededCount"),
        @JsonSubTypes.Type(value = EmptyFieldCheckParam.class, name = "EmptyField"),
        @JsonSubTypes.Type(value = UnderPopulatedFieldCheckParam.class, name = "UnderPopulatedField"),
        @JsonSubTypes.Type(value = IncompleteCoverageForColChkParam.class, name = "IncompleteCoverageForCol"),
        @JsonSubTypes.Type(value = OutOfCoverageForRowChkParam.class, name = "OutOfCoverageValForRow"),
        @JsonSubTypes.Type(value = ExceedDomDiffBetwenVersionChkParam.class, name = "ExceededVersionDiffForDomOnly"),
        @JsonSubTypes.Type(value = ExceedCntDiffBetwenVersionChkParam.class, name = "ExceededVersionDiffForNumOfBusinesses")
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
