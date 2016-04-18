package com.latticeengines.domain.exposed.propdata.manage;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "Transformation")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Transformation implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", nullable = true)
    private Long pid;

    @Column(name = "TransformationName", unique = true, nullable = false, length = 100)
    private String transformationName;

    @Column(name = "SourceName", nullable = false, length = 100)
    private String sourceName;

    @Override
    @JsonProperty("PID")
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonProperty("PID")
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("TransformationName")
    public String getTransformationName() {
        return transformationName;
    }

    @JsonProperty("TransformationName")
    public void setTransformationName(String transformationName) {
        this.transformationName = transformationName;
    }

    @JsonProperty("SourceName")
    public String getSourceName() {
        return sourceName;
    }

    @JsonProperty("SourceName")
    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
