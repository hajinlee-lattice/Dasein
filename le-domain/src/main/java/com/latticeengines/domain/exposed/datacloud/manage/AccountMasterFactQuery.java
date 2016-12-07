package com.latticeengines.domain.exposed.datacloud.manage;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AccountMasterFactQuery {

    @JsonProperty("LocationQuery")
    private DimensionalQuery locationQry;

    @JsonProperty("IndustryQuery")
    private DimensionalQuery industryQry;

    @JsonProperty("NumEmpRangeQuery")
    private DimensionalQuery numEmpRangeQry;

    @JsonProperty("RevRangeQry")
    private DimensionalQuery revRangeQry;

    @JsonProperty("NumLocRangeQry")
    private DimensionalQuery numLocRangeQry;

    @JsonProperty("CategoryQry")
    private DimensionalQuery categoryQry;

    public DimensionalQuery getLocationQry() {
        return locationQry;
    }

    public void setLocationQry(DimensionalQuery locationQry) {
        this.locationQry = locationQry;
    }

    public DimensionalQuery getIndustryQry() {
        return industryQry;
    }

    public void setIndustryQry(DimensionalQuery industryQry) {
        this.industryQry = industryQry;
    }

    public DimensionalQuery getNumEmpRangeQry() {
        return numEmpRangeQry;
    }

    public void setNumEmpRangeQry(DimensionalQuery numEmpRangeQry) {
        this.numEmpRangeQry = numEmpRangeQry;
    }

    public DimensionalQuery getRevRangeQry() {
        return revRangeQry;
    }

    public void setRevRangeQry(DimensionalQuery revRangeQry) {
        this.revRangeQry = revRangeQry;
    }

    public DimensionalQuery getNumLocRangeQry() {
        return numLocRangeQry;
    }

    public void setNumLocRangeQry(DimensionalQuery numLocRangeQry) {
        this.numLocRangeQry = numLocRangeQry;
    }

    public DimensionalQuery getCategoryQry() {
        return categoryQry;
    }

    public void setCategoryQry(DimensionalQuery categoryQry) {
        this.categoryQry = categoryQry;
    }
}
