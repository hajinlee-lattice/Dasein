package com.latticeengines.domain.exposed.query.frontend;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.PageFilter;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class FrontEndQuery {

    @JsonProperty("frontend_restriction")
    private FrontEndRestriction frontEndRestriction;

    @JsonProperty("sort")
    private FrontEndSort sort;

    @JsonProperty("page_filter")
    private PageFilter pageFilter;

    @JsonProperty("free_form_text_search")
    private String freeFormTextSearch;

    @JsonProperty("restrict_without_sfdcid")
    @ApiModelProperty("Restrict to accounts without Salesforce id.")
    private boolean restrictNullSalesforceId = false;

    @JsonProperty("restrict_with_sfdcid")
    @ApiModelProperty("Restrict to accounts with Salesforce id.")
    private boolean restrictNotNullSalesforceId = false;

    public FrontEndRestriction getFrontEndRestriction() {
        return frontEndRestriction;
    }

    public void setFrontEndRestriction(FrontEndRestriction frontEndRestriction) {
        this.frontEndRestriction = frontEndRestriction;
    }

    public FrontEndSort getSort() {
        return sort;
    }

    public void setSort(FrontEndSort sort) {
        this.sort = sort;
    }

    public PageFilter getPageFilter() {
        return pageFilter;
    }

    public void setPageFilter(PageFilter pageFilter) {
        this.pageFilter = pageFilter;
    }

    public String getFreeFormTextSearch() {
        return freeFormTextSearch;
    }

    public void setFreeFormTextSearch(String freeFormTextSearch) {
        this.freeFormTextSearch = freeFormTextSearch;
    }

    public boolean restrictNullSalesforceId() {
        return restrictNullSalesforceId;
    }

    public void setRestrictNullSalesforceId(boolean restrictNullSalesforceId) {
        this.restrictNullSalesforceId = restrictNullSalesforceId;
    }

    public boolean restrictNotNullSalesforceId() {
        return restrictNotNullSalesforceId;
    }

    public void setRestrictNotNullSalesforceId(boolean restrictNotNullSalesforceId) {
        this.restrictNotNullSalesforceId = restrictNotNullSalesforceId;
    }

}
