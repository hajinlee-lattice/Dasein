package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "MODEL_SUMMARY_DOWNLOAD_FLAGS")
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

}
