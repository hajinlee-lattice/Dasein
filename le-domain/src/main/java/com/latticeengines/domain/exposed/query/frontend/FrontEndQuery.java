package com.latticeengines.domain.exposed.query.frontend;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class FrontEndQuery {

    @JsonProperty("account_restriction")
    private FrontEndRestriction accountRestriction;

    @JsonProperty("contact_restriction")
    private FrontEndRestriction contactRestriction;

    @JsonProperty("account_contact_operator")
    private LogicalOperator accountContactOperator;

    @JsonProperty("sort")
    private FrontEndSort sort;

    @JsonProperty("page_filter")
    private PageFilter pageFilter;

    @JsonProperty("rating_models")
    private List<RatingModel> ratingModels;

    @JsonProperty("free_form_text_search")
    private String freeFormTextSearch;

    @JsonProperty("restrict_without_sfdcid")
    @ApiModelProperty("Restrict to accounts without Salesforce id.")
    private boolean restrictNullSalesforceId = false;

    @JsonProperty("restrict_with_sfdcid")
    @ApiModelProperty("Restrict to accounts with Salesforce id.")
    private boolean restrictNotNullSalesforceId = false;

    @JsonProperty("lookups")
    private List<Lookup> lookups = new ArrayList<>();

    @JsonProperty("main_entity")
    private BusinessEntity mainEntity;

    public FrontEndRestriction getAccountRestriction() {
        return accountRestriction;
    }

    public void setAccountRestriction(FrontEndRestriction accountRestriction) {
        this.accountRestriction = accountRestriction;
    }

    public FrontEndRestriction getContactRestriction() {
        return contactRestriction;
    }

    public void setContactRestriction(FrontEndRestriction contactRestriction) {
        this.contactRestriction = contactRestriction;
    }

    public LogicalOperator getAccountContactOperator() {
        return accountContactOperator;
    }

    public void setAccountContactOperator(LogicalOperator accountContactOperator) {
        this.accountContactOperator = accountContactOperator;
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

    public List<RatingModel> getRatingModels() {
        return ratingModels;
    }

    public void setRatingModels(List<RatingModel> ratingModels) {
        this.ratingModels = ratingModels;
    }

    public List<Lookup> getLookups() {
        return lookups;
    }

    public void setLookups(List<Lookup> lookups) {
        this.lookups = lookups;
    }

    public BusinessEntity getMainEntity() {
        return mainEntity;
    }

    public void setMainEntity(BusinessEntity mainEntity) {
        this.mainEntity = mainEntity;
    }

    public void addLookups(BusinessEntity businessEntity, String... attrNames) {
        List<Lookup> moreLookups = new ArrayList<>(Arrays.asList(attrNames)).stream() //
                .map((attrName) -> new AttributeLookup(businessEntity, attrName)) //
                .collect(Collectors.toList());
        lookups.addAll(moreLookups);
    }

}
