package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ATLAS_CATALOG", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "NAME", "FK_TENANT_ID" }), //
        @UniqueConstraint(columnNames = { "FK_TASK_ID", "FK_TENANT_ID" }) })
public class Catalog implements HasPid, Serializable, HasAuditingFields {

    private static final long serialVersionUID = -8455242959058500143L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("tenant")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "`FK_TASK_ID`", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DataFeedTask dataFeedTask;

    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("created")
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("updated")
    private Date updated;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public DataFeedTask getDataFeedTask() {
        return dataFeedTask;
    }

    public void setDataFeedTask(DataFeedTask dataFeedTask) {
        this.dataFeedTask = dataFeedTask;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date creationTime) {
        this.created = creationTime;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updateTime) {
        this.updated = updateTime;
    }

    @Override
    public String toString() {
        return "Catalog{" + "pid=" + pid + ", name='" + name + '\'' + ", tenant=" + tenant + ", dataFeedTask="
                + dataFeedTask + ", created=" + created + ", updated=" + updated + '}';
    }
}
