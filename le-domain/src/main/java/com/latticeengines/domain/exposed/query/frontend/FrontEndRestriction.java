package com.latticeengines.domain.exposed.query.frontend;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BucketRestriction;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel("Group selected buckets into two groups, one for OR and one for AND. " +
        "There is an outer AND join them together.")
public class FrontEndRestriction {

    @JsonProperty("any")
    @ApiModelProperty("These selections will become a big OR group.")
    private List<BucketRestriction> any = new ArrayList<>();

    @JsonProperty("all")
    @ApiModelProperty("These selections will become a big AND group.")
    private List<BucketRestriction> all = new ArrayList<>();

    public List<BucketRestriction> getAny() {
        return any;
    }

    public void setAny(List<BucketRestriction> any) {
        this.any = any;
    }

    public List<BucketRestriction> getAll() {
        return all;
    }

    public void setAll(List<BucketRestriction> all) {
        this.all = all;
    }

}
