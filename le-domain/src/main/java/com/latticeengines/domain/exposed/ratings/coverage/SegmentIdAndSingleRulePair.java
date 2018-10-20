package com.latticeengines.domain.exposed.ratings.coverage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SegmentIdAndSingleRulePair {

    @JsonProperty("responseKeyId")
    private String responseKeyId;

    @JsonProperty("segmentId")
    private String segmentId;

    @JsonProperty(FrontEndQueryConstants.ACCOUNT_RESTRICTION)
    private Restriction accountRestriction;

    @JsonProperty(FrontEndQueryConstants.CONTACT_RESTRICTION)
    private Restriction contacttRestriction;

    public String getResponseKeyId() {
        return responseKeyId;
    }

    public void setResponseKeyId(String responseKeyId) {
        this.responseKeyId = responseKeyId;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(String segmentId) {
        this.segmentId = segmentId;
    }

    public Restriction getAccountRestriction() {
        return accountRestriction;
    }

    public void setAccountRestriction(Restriction accountRestriction) {
        this.accountRestriction = accountRestriction;
    }

    public Restriction getContacttRestriction() {
        return contacttRestriction;
    }

    public void setContacttRestriction(Restriction contacttRestriction) {
        this.contacttRestriction = contacttRestriction;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
