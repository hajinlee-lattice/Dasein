package com.latticeengines.domain.exposed.datacloud.match.config;

// https://directplus.documentation.dnb.com/openAPI.html?apiID=IDRCleanseMatch
public enum ExclusionCriterion {
    NonHeadQuarters("ExcludeNonHeadQuarters"), //
    NonMarketable("ExcludeNonMarketable"), //
    OutOfBusiness("ExcludeOutofBusiness"), //
    Undeliverable("ExcludeUndeliverable"), //
    Unreachable("ExcludeUnreachable");

    private final String urlParam;

    ExclusionCriterion(String urlParam) {
        this.urlParam = urlParam;
    }

    public String getUrlParam() {
        return urlParam;
    }
}
