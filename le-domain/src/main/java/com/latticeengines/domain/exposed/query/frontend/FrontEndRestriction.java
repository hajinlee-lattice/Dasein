package com.latticeengines.domain.exposed.query.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.Restriction;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel("For simple queries, group selected buckets into two groups, one for OR and one for AND. "
        + "Then use an outer AND to join them together.  For advanced queries, use whatever nesting as specified "
        + "by user")
public class FrontEndRestriction {

    @JsonProperty("restriction")
    @ApiModelProperty("This restriction can represent both simple and advanced queries.")
    private Restriction restriction;


    public Restriction getRestriction() {
        return restriction;
    }

    public void setRestriction(Restriction restriction) {
        this.restriction = restriction;
    }
}
