package com.latticeengines.domain.exposed.ratings.coverage;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingModelsCoverageRequest {

    private List<String> ratingModelIds;

    private boolean restrictNotNullSalesforceId;

    private boolean loadContactsCoverage;

    private boolean loadContactsBucketCoverage;

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

    public boolean isLoadContactsCoverage() {
        return loadContactsCoverage;
    }

    public void setLoadContactsCoverage(boolean loadContactsCoverage) {
        this.loadContactsCoverage = loadContactsCoverage;
    }

    public boolean isLoadContactsBucketCoverage() {
        return loadContactsBucketCoverage;
    }

    public void setLoadContactsBucketCoverage(boolean loadContactsBucketCoverage) {
        this.loadContactsBucketCoverage = loadContactsBucketCoverage;
    }

}
