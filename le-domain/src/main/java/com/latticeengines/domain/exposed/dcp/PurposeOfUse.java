package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PurposeOfUse {

    @JsonProperty("domain")
    private DataDomain domain;

    @JsonProperty("recordType")
    private DataRecordType recordType;

    public DataDomain getDomain() {
        return domain;
    }

    public void setDomain(DataDomain domain) {
        this.domain = domain;
    }

    public DataRecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(DataRecordType recordType) {
        this.recordType = recordType;
    }
}
