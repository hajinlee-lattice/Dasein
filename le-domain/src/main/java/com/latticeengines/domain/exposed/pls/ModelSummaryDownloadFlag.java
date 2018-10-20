package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODEL_SUMMARY_DOWNLOAD_FLAGS", indexes = { //
        @Index(name = "IX_TENANT_ID", columnList = "Tenant_ID"), //
        @Index(name = "IX_MARK_TIME", columnList = "MARK_TIME") //
})
public class ModelSummaryDownloadFlag implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "PID", nullable = false)
    private Long pid;

    @JsonProperty("tenant_id")
    @Column(name = "Tenant_ID")
    private String tenantId;

    @JsonProperty("mark_time")
    @Column(name = "MARK_TIME")
    private Date markTime;

    @JsonProperty("exclude_tenant_id")
    @Column(name = "EXCLUDE_TENANT_ID")
    private String excludeTenantID;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public Date getMarkTime() {
        return markTime;
    }

    public void setMarkTime(Date markTime) {
        this.markTime = markTime;
    }

    public String getExcludeTenantID() {
        return excludeTenantID;
    }

    public void setExcludeTenantID(String tenantId) {
        this.excludeTenantID = tenantId;
    }
}
