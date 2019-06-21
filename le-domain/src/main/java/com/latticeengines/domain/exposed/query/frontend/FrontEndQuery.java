package com.latticeengines.domain.exposed.query.frontend;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class FrontEndQuery {

    @JsonProperty(FrontEndQueryConstants.ACCOUNT_RESTRICTION)
    private FrontEndRestriction accountRestriction;

    @JsonProperty(FrontEndQueryConstants.CONTACT_RESTRICTION)
    private FrontEndRestriction contactRestriction;

    @JsonProperty(FrontEndQueryConstants.SORT)
    private FrontEndSort sort;

    @JsonProperty(FrontEndQueryConstants.PAGE_FILTER)
    private PageFilter pageFilter;

    @JsonProperty(FrontEndQueryConstants.RATING_MODELS)
    private List<RatingModel> ratingModels;

    @JsonProperty(FrontEndQueryConstants.FREE_FORM_TEXT_SEARCH)
    private String freeFormTextSearch;

    @JsonProperty(FrontEndQueryConstants.RESTRICT_WITHOUT_SFDCID)
    @ApiModelProperty("Restrict to accounts without Salesforce id.")
    private boolean restrictNullSalesforceId = false;

    @JsonProperty(FrontEndQueryConstants.RESTRICT_WITH_SFDCID)
    @ApiModelProperty("Restrict to accounts with Salesforce id.")
    private boolean restrictNotNullSalesforceId = false;

    @JsonProperty(FrontEndQueryConstants.LOOKUPS)
    private List<Lookup> lookups = new ArrayList<>();

    @JsonProperty(FrontEndQueryConstants.MAIN_ENTITY)
    private BusinessEntity mainEntity;

    @JsonProperty(FrontEndQueryConstants.PREEXISTING_SEGMENT_NAME)
    private String preexistingSegmentName;

    @JsonProperty(FrontEndQueryConstants.EVALUATION_DATE)
    private String evaluationDateStr;

    @JsonProperty(FrontEndQueryConstants.DISTINCT)
    private boolean distinct = false;

    public static FrontEndQuery fromSegment(MetadataSegment segment) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        FrontEndRestriction accountRestriction = null;
        if (segment.getAccountRestriction() != null) {
            accountRestriction = new FrontEndRestriction(segment.getAccountRestriction());
        } else if (segment.getAccountFrontEndRestriction() != null) {
            accountRestriction = segment.getAccountFrontEndRestriction();
        }
        frontEndQuery.setAccountRestriction(accountRestriction);

        FrontEndRestriction contactRestriction = null;
        if (segment.getContactRestriction() != null) {
            contactRestriction = new FrontEndRestriction(segment.getContactRestriction());
        } else if (segment.getContactFrontEndRestriction() != null) {
            contactRestriction = segment.getContactFrontEndRestriction();
        }
        frontEndQuery.setContactRestriction(contactRestriction);

        return frontEndQuery;
    }

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

    public boolean getDistinct() {
        return this.distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
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

    public String getPreexistingSegmentName() {
        return preexistingSegmentName;
    }

    public void setPreexistingSegmentName(String preexistingSegmentName) {
        this.preexistingSegmentName = preexistingSegmentName;
    }

    public String getEvaluationDateStr() {
        return evaluationDateStr;
    }

    public void setEvaluationDateStr(String evaluationDateStr) {
        this.evaluationDateStr = evaluationDateStr;
    }

    public void addLookups(BusinessEntity businessEntity, String... attrNames) {
        List<Lookup> moreLookups = new ArrayList<>(Arrays.asList(attrNames)).stream() //
                .map((attrName) -> new AttributeLookup(businessEntity, attrName)) //
                .collect(Collectors.toList());
        lookups.addAll(moreLookups);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public FrontEndQuery getDeepCopy() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, this);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return KryoUtils.read(bis, FrontEndQuery.class);
    }

}
