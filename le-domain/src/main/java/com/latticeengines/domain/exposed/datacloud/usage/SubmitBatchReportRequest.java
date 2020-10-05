package com.latticeengines.domain.exposed.datacloud.usage;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class SubmitBatchReportRequest {

    // an identifier for this batch
    @JsonProperty("batchRef")
    private String batchRef;

    @JsonProperty("numRecords")
    private Long numRecords;

    public String getBatchRef() {
        return batchRef;
    }

    public void setBatchRef(String batchRef) {
        this.batchRef = batchRef;
    }

    public Long getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(Long numRecords) {
        this.numRecords = numRecords;
    }
}
