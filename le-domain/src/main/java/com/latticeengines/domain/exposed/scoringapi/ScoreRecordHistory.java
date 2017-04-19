package com.latticeengines.domain.exposed.scoringapi;

import io.swagger.annotations.ApiModelProperty;

import javax.persistence.Id;

import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.Union;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.RedisIndex;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class ScoreRecordHistory implements HasId<String> {

    @Id
    @Nullable
    @AvroName("ID")
    String id;

    @JsonProperty("latticeId")
    @ApiModelProperty(value = "Lattice Id of score request")
    @RedisIndex(name = "latticeId")
    @Nullable
    @AvroName("LatticeId")
    private String latticeId;

    @JsonProperty("recordId")
    @ApiModelProperty(value = "Record ID")
    @Nullable
    @AvroName("RecordId")
    private String recordId;

    @JsonProperty("idType")
    @ApiModelProperty(value = "Type of record ID")
    @Nullable
    @AvroName("IdType")
    private String idType;

    @JsonProperty("requestTimestamp")
    @ApiModelProperty(value = "Timestamp request is received")
    @Nullable
    @AvroName("RequestTimestamp")
    private String requestTimestamp;

    @JsonProperty("request")
    @ApiModelProperty(value = "Score request")
    @Nullable
    @AvroName("Request")
    private String request;

    @JsonProperty("response")
    @ApiModelProperty(value = "Score response")
    @Nullable
    @AvroName("Response")
    private String response;

    @JsonProperty("tenantId")
    @ApiModelProperty(value = "Tenant Id")
    @Nullable
    @AvroName("TenantId")
    private String tenantId;

    @Override
    @Union({})
    public String getId() {
        return id;
    }

    @Override
    @Union({})
    public void setId(String id) {
        this.id = id;
    }

    @Union({})
    public String getLatticeId() {
        return latticeId;
    }

    @Union({})
    public void setLatticeId(String latticeId) {
        this.latticeId = latticeId;
    }

    @Union({})
    public String getRecordId() {
        return recordId;
    }

    @Union({})
    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    @Union({})
    public String getIdType() {
        return idType;
    }

    @Union({})
    public void setIdType(String idType) {
        this.idType = idType;
    }

    @Union({})
    public String getRequestTimestamp() {
        return requestTimestamp;
    }

    @Union({})
    public void setRequestTimestamp(String requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }

    @Union({})
    public String getRequest() {
        return request;
    }

    @Union({})
    public void setRequest(String request) {
        this.request = request;
    }

    @Union({})
    public String getResponse() {
        return response;
    }

    @Union({})
    public void setResponse(String response) {
        this.response = response;
    }

    @Union({})
    public String getTenantId() {
        return tenantId;
    }

    @Union({})
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
}
