package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT)
@JsonSubTypes({ //
        @Type(value = SalesforceChannelConfig.class, name = "salesforce"), //
        @Type(value = MarketoChannelConfig.class, name = "marketo"), //
        @Type(value = EloquaChannelConfig.class, name = "eloqua"), //
        @Type(value = S3ChannelConfig.class, name = "s3"), //
})
public interface ChannelConfig {

    // @JsonProperty("limitAccounts")
    // private Boolean limitAccounts = Boolean.FALSE;
    //
    // @JsonProperty("limitContacts")
    // private Boolean limitContacts = Boolean.FALSE;
    //
    // @JsonProperty("selectRatingIfContainsModel")
    // private Boolean selectRatingIfContainsModel = Boolean.FALSE;
    //
    // @JsonProperty("selectUnscored")
    // private Boolean selectUnscored = Boolean.FALSE;
    //
    // public Boolean isLimitAccounts() {
    // return limitAccounts;
    // }
    //
    // public void setLimitAccounts(boolean limitAccounts) {
    // this.limitAccounts = limitAccounts;
    // }
    //
    // public Boolean isLimitContacts() {
    // return limitContacts;
    // }
    //
    // public void setLimitContacts(boolean limitContacts) {
    // this.limitContacts = limitContacts;
    // }
    //
    // public Boolean isSelectRatingIfContainsModel() {
    // return selectRatingIfContainsModel;
    // }
    //
    // public void setSelectRatingIfContainsModel(boolean
    // selectRatingIfContainsModel) {
    // this.selectRatingIfContainsModel = selectRatingIfContainsModel;
    // }
    //
    // public Boolean isSelectUnscored() {
    // return selectUnscored;
    // }
    //
    // public void setSelectUnscored(boolean selectUnscored) {
    // this.selectUnscored = selectUnscored;
    // }

    ChannelConfig copyConfig(ChannelConfig config);
    // this.setLimitAccounts(config.isLimitAccounts());
    // this.setLimitContacts(config.isLimitContacts());
    // this.setSelectRatingIfContainsModel(config.isSelectRatingIfContainsModel());
    // this.setSelectUnscored(config.isSelectUnscored());
    // return this;

}
