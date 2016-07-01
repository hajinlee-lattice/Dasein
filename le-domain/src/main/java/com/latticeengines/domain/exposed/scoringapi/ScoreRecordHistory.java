package com.latticeengines.domain.exposed.scoringapi;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import javax.persistence.Id;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.datafabric.RedisIndex;
import com.latticeengines.domain.exposed.dataplatform.HasId;
//import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Record;
import io.swagger.annotations.ApiModelProperty;

public class ScoreRecordHistory implements HasId<String> {

    @Id
    String id;

    @JsonProperty("latticeId")
    @ApiModelProperty(value = "Lattice Id of score request")
    @RedisIndex(name = "latticeId")
    private String latticeId;

    @JsonProperty("recordId")
    @ApiModelProperty(value = "Record ID")
    private String recordId;

    @JsonProperty("idType")
    @ApiModelProperty(value = "Type of record ID")
    private String idType;

    @JsonProperty("requestTimestamp")
    @ApiModelProperty(value = "Timestamp request is received")
    private String requestTimestamp;

    @JsonProperty("request")
    @ApiModelProperty(value = "Score request")
    private String request;
 
    @JsonProperty("response")
    @ApiModelProperty(value = "Score response")
    private String response;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLatticeId() {
        return latticeId;
    }

    public void setLatticeId(String latticeId) {
        this.latticeId = latticeId;
    }

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getIdType() {
        return idType;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public String getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(String requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }
}
