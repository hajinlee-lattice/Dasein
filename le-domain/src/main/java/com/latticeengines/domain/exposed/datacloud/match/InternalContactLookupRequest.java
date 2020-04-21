package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class InternalContactLookupRequest {

    @JsonProperty("CustomerSpace")
    private String customerSpace;

    @JsonProperty("DataCollectionVersion")
    private DataCollection.Version dataCollectionVersion;

    @JsonProperty("ContactId")
    private String contactId;

    @JsonProperty("AccountLookupId")
    private String accountLookupId;

    @JsonProperty("AccountLookupIdVal")
    private String accountLookupIdVal;

    public String getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }

    public DataCollection.Version getDataCollectionVersion() {
        return dataCollectionVersion;
    }

    public void setDataCollectionVersion(DataCollection.Version dataCollectionVersion) {
        this.dataCollectionVersion = dataCollectionVersion;
    }

    public String getContactId() {
        return contactId;
    }

    public void setContactId(String contactId) {
        this.contactId = contactId;
    }

    public String getAccountLookupId() {
        return accountLookupId;
    }

    public void setAccountLookupId(String accountLookupId) {
        this.accountLookupId = accountLookupId;
    }

    public String getAccountLookupIdVal() {
        return accountLookupIdVal;
    }

    public void setAccountLookupIdVal(String accountLookupIdVal) {
        this.accountLookupIdVal = accountLookupIdVal;
    }

}
