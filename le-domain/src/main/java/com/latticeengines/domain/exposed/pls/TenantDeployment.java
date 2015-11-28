package com.latticeengines.domain.exposed.pls;

import java.sql.Timestamp;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "TENANT_DEPLOYMENT")
public class TenantDeployment implements HasPid {

    private Long pid;
    private Long tenantId;
    private Timestamp createTime;
    private String createdBy;
    private TenantDeploymentStep step;
    private TenantDeploymentStatus status;
    private Long currentLaunchId;
    private String modelId;
    private String message;

    @Override
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("TenantId")
    @Column(name = "TENANT_ID", nullable = false)
    public Long getTenantId() {
        return tenantId;
    }

    @JsonProperty("TenantId")
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @JsonProperty("CreateTime")
    @Column(name = "CREATE_TIME", nullable = false)
    public Timestamp getCreateTime() {
        return createTime;
    }

    @JsonProperty("CreateTime")
    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    @JsonProperty("CreatedBy")
    @Column(name = "CREATED_BY", nullable = false)
    public String getCreatedBy() {
        return createdBy;
    }

    @JsonProperty("CreatedBy")
    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @JsonProperty("Step")
    @Column(name = "STEP", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    public TenantDeploymentStep getStep() {
        return step;
    }

    @JsonProperty("Step")
    public void setStep(TenantDeploymentStep step) {
        this.step = step;
    }

    @JsonProperty("Status")
    @Column(name = "STATUS", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    public TenantDeploymentStatus getStatus() {
        return status;
    }

    @JsonProperty("Status")
    public void setStatus(TenantDeploymentStatus status) {
        this.status = status;
    }

    @JsonProperty("CurrentLaunchId")
    @Column(name = "CURRENT_LAUNCH_ID")
    public Long getCurrentLaunchId() {
        return currentLaunchId;
    }

    @JsonProperty("CurrentLaunchId")
    public void setCurrentLaunchId(Long currentLaunchId) {
        this.currentLaunchId = currentLaunchId;
    }

    @JsonProperty("ModelId")
    @Column(name = "MODEL_ID")
    public String getModelId() {
        return modelId;
    }

    @JsonProperty("ModelId")
    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    @JsonProperty("Message")
    @Column(name = "MESSAGE")
    @Lob
    public String getMessage() {
        return message;
    }

    @JsonProperty("Message")
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
