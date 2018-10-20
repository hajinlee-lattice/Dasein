package com.latticeengines.domain.exposed.ratings.coverage;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingModelsCoverageRequest {

    private List<String> ratingModelIds;

    private boolean restrictNotNullSalesforceId;

    private boolean loadContactsCount;

    private boolean loadContactsCountByBucket;

    public List<String> getRatingModelIds() {
        return ratingModelIds;
    }

    public void setRatingModelIds(List<String> ratingModelIds) {
        this.ratingModelIds = ratingModelIds;
    }

    public boolean isRestrictNotNullSalesforceId() {
        return restrictNotNullSalesforceId;
    }

    public void setRestrictNotNullSalesforceId(boolean restrictNotNullSalesforceId) {
        this.restrictNotNullSalesforceId = restrictNotNullSalesforceId;
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


}
