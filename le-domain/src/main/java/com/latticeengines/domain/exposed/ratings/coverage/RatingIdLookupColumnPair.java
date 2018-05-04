package com.latticeengines.domain.exposed.ratings.coverage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingIdLookupColumnPair {
    private String responseKeyId;

    private String ratingEngineId;

    private String lookupColumn;

    public String getResponseKeyId() {
        return responseKeyId;
    }

    public void setResponseKeyId(String responseKeyId) {
        this.responseKeyId = responseKeyId;
    }

    public String getRatingEngineId() {
        return ratingEngineId;
    }

    public void setRatingEngineId(String ratingEngineId) {
        this.ratingEngineId = ratingEngineId;
    }

    public String getLookupColumn() {
        return lookupColumn;
    }

    public void setLookupColumn(String lookupColumn) {
        this.lookupColumn = lookupColumn;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
