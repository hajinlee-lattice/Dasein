package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;

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

    public String getSegmentId() {
        return segmentId;
    }

    public Restriction getAccountRestriction() {
        return accountRestriction;
    }

    public Restriction getContacttRestriction() {
        return contacttRestriction;
    }

    public void setResponseKeyId(String responseKeyId) {
        this.responseKeyId = responseKeyId;
    }

    public void setSegmentId(String segmentId) {
        this.segmentId = segmentId;
    }

    public void setAccountRestriction(Restriction accountRestriction) {
        this.accountRestriction = accountRestriction;
    }

    public void setContacttRestriction(Restriction contacttRestriction) {
        this.contacttRestriction = contacttRestriction;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
