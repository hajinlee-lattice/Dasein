package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
    the object is used to mark whether one node is only child in dcp hierarchy, if so,
    update its parental report in dcp import work flow
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DunsCountCopy {

    @JsonProperty("isOnlyChild")
    private boolean isOnlyChild;

    @JsonProperty("parentOwnerId")
    private String parentOwnerId;

    public boolean isOnlyChild() {
        return isOnlyChild;
    }

    public void setOnlyChild(boolean onlyChild) {
        isOnlyChild = onlyChild;
    }

    public String getParentOwnerId() {
        return parentOwnerId;
    }

    public void setParentOwnerId(String parentOwnerId) {
        this.parentOwnerId = parentOwnerId;
    }
}
