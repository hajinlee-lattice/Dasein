package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LatticeIdAssignFlowParameter extends TransformationFlowParameters {
    @JsonProperty("AMSeedIdField")
    private String amSeedIdField;

    @JsonProperty("AMSeedDunsField")
    private String amSeedDunsField;

    @JsonProperty("AMSeedDomainField")
    private String amSeedDomainField;

    @JsonProperty("AMIdSrcIdField")
    private String amIdSrcIdField;

    @JsonProperty("AMIdSrcDunsField")
    private String amIdSrcDunsField;

    @JsonProperty("AMIdSrcDomainField")
    private String amIdSrcDomainField;

    @JsonProperty("CurrentCount")
    private Long currentCount;

    public String getAmSeedIdField() {
        return amSeedIdField;
    }

    public void setAmSeedIdField(String amSeedIdField) {
        this.amSeedIdField = amSeedIdField;
    }

    public String getAmIdSrcIdField() {
        return amIdSrcIdField;
    }

    public void setAmIdSrcIdField(String amIdSrcIdField) {
        this.amIdSrcIdField = amIdSrcIdField;
    }

    public String getAmSeedDunsField() {
        return amSeedDunsField;
    }

    public void setAmSeedDunsField(String amSeedDunsField) {
        this.amSeedDunsField = amSeedDunsField;
    }

    public String getAmSeedDomainField() {
        return amSeedDomainField;
    }

    public void setAmSeedDomainField(String amSeedDomainField) {
        this.amSeedDomainField = amSeedDomainField;
    }

    public String getAmIdSrcDunsField() {
        return amIdSrcDunsField;
    }

    public void setAmIdSrcDunsField(String amIdSrcDunsField) {
        this.amIdSrcDunsField = amIdSrcDunsField;
    }

    public String getAmIdSrcDomainField() {
        return amIdSrcDomainField;
    }

    public void setAmIdSrcDomainField(String amIdSrcDomainField) {
        this.amIdSrcDomainField = amIdSrcDomainField;
    }

    public Long getCurrentCount() {
        return currentCount;
    }

    public void setCurrentCount(Long currentCount) {
        this.currentCount = currentCount;
    }

}
