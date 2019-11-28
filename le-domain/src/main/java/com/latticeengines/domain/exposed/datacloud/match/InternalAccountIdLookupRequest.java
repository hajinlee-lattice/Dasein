package com.latticeengines.domain.exposed.datacloud.match;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class InternalAccountIdLookupRequest {

    @JsonProperty("CustomerSpace")
    private String customerSpace;

    @JsonProperty("DataCollectionVersion")
    private DataCollection.Version dataCollectionVersion;

    @JsonProperty("LookupId")
    private String lookupId;

    @JsonProperty("LookupIdVal")
    private String lookupIdVal;

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

    public String getLookupId() {
        return lookupId;
    }

    public void setLookupId(String lookupId) {
        this.lookupId = lookupId;
    }

    public String getLookupIdVal() {
        return lookupIdVal;
    }

    public void setLookupIdVal(String lookupIdVal) {
        this.lookupIdVal = lookupIdVal;
    }
}
