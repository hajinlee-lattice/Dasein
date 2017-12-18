package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AmReportGenerateParams extends TransformationFlowParameters {
    @JsonProperty("CheckCode")
    private String checkCode;

    @JsonProperty("RowId")
    private String rowId;

    @JsonProperty("GroupId")
    private String groupId;

    @JsonProperty("CheckValue")
    private String checkValue;

    @JsonProperty("CheckMessage")
    private String checkMessage;

    @JsonProperty("CheckField")
    private String checkField;

    @JsonProperty("Count")
    private Long count;

    public String getCheckCode() {
        return checkCode;
    }

    public void setCheckCode(String checkCode) {
        this.checkCode = checkCode;
    }

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getCheckValue() {
        return checkValue;
    }

    public void setCheckValue(String checkValue) {
        this.checkValue = checkValue;
    }

    public String getCheckMessage() {
        return checkMessage;
    }

    public void setCheckMessage(String checkMessage) {
        this.checkMessage = checkMessage;
    }

    public String getCheckField() {
        return checkField;
    }

    public void setCheckField(String checkField) {
        this.checkField = checkField;
    }

    public Long getSrcCount() {
        return count;
    }

    public void setSrcCount(Long count) {
        this.count = count;
    }

}
