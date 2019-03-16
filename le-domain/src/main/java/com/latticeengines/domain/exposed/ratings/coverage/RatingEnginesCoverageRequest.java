package com.latticeengines.domain.exposed.ratings.coverage;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingEnginesCoverageRequest {

    private List<String> ratingEngineIds;

    private boolean restrictNullLookupId;

    private boolean loadContactsCount;

    private boolean loadContactsCountByBucket;
    
    private boolean loadContactsWithoutEmailCount;

    private String lookupId;

    public List<String> getRatingEngineIds() {
        return ratingEngineIds;
    }

    public void setRatingEngineIds(List<String> ratingEngineIds) {
        this.ratingEngineIds = ratingEngineIds;
    }

    public boolean isRestrictNullLookupId() {
        return restrictNullLookupId;
    }

    public void setRestrictNullLookupId(boolean restrictNullLookupId) {
        this.restrictNullLookupId = restrictNullLookupId;
    }

    public boolean isLoadContactsCount() {
        return loadContactsCount;
    }

    public void setLoadContactsCount(boolean loadContactsCount) {
        this.loadContactsCount = loadContactsCount;
    }

    public boolean isLoadContactsCountByBucket() {
        return loadContactsCountByBucket;
    }

    public void setLoadContactsCountByBucket(boolean loadContactsCountByBucket) {
        this.loadContactsCountByBucket = loadContactsCountByBucket;
    }

    public String getLookupId() {
        return lookupId;
    }

    public void setLookupId(String lookupId) {
        this.lookupId = lookupId;
    }
    
    public boolean isLoadContactsWithoutEmailCount() {
        return loadContactsWithoutEmailCount;
    }

    public void setLoadContactsWithoutEmailCount(boolean loadContactsWithoutEmailCount) {
        this.loadContactsWithoutEmailCount = loadContactsWithoutEmailCount;
    }


}
