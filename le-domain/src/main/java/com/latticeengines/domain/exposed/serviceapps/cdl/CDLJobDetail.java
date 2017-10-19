package com.latticeengines.domain.exposed.serviceapps.cdl;

import java.io.Serializable;
import java.util.Date;
import java.util.EnumSet;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@Table(name = "CDL_JOB_DETAIL")
public class CDLJobDetail implements HasPid, Serializable {

    private static final long serialVersionUID = 7530709453299469324L;

    public static final EnumSet<CDLJobStatus> TERMINATED_STATUS = EnumSet.of(CDLJobStatus.FAIL,
            CDLJobStatus.COMPLETE);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("cdl_job_type")
    @Column(name = "CDL_JOB_TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    private CDLJobType cdlJobType;

    @JsonProperty("cdl_job_status")
    @Column(name = "CDL_JOB_STATUS", nullable = false)
    @Enumerated(EnumType.STRING)
    private CDLJobStatus cdlJobStatus;

    @JsonProperty("retry_count")
    @Column(name = "RETRY_COUNT", nullable = false)
    private int retryCount = 0;

    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("create_date")
    @Column(name = "CREATE_DATE")
    private Date createDate;

    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("last_update_date")
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdateDate;

    @JsonProperty("application_id")
    @Column(name = "APPLICATION_ID")
    private String applicationId;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public CDLJobType getCdlJobType() {
        return cdlJobType;
    }

    public void setCdlJobType(CDLJobType cdlJobType) {
        this.cdlJobType = cdlJobType;
    }

    public CDLJobStatus getCdlJobStatus() {
        return cdlJobStatus;
    }

    public void setCdlJobStatus(CDLJobStatus cdlJobStatus) {
        this.cdlJobStatus = cdlJobStatus;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public Date getLastUpdateDate() {
        return lastUpdateDate;
    }

    public void setLastUpdateDate(Date lastUpdateDate) {
        this.lastUpdateDate = lastUpdateDate;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @JsonIgnore
    public Tenant getTenant() {
        return tenant;
    }

    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        setTenantId(tenant.getPid());
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Transient
    @JsonIgnore
    public boolean isRunning() {
        return !TERMINATED_STATUS.contains(getCdlJobStatus());
    }
}
