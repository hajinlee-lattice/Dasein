package com.latticeengines.domain.exposed.cdl.activity;

import static com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.IngestionBehavior.Upsert;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

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
import com.google.common.base.Objects;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ATLAS_CATALOG", uniqueConstraints = { //
        @UniqueConstraint(columnNames = { "NAME", "FK_TENANT_ID" }), //
        @UniqueConstraint(columnNames = { "CATALOG_ID", "FK_TENANT_ID" }), //
        @UniqueConstraint(columnNames = { "FK_TASK_ID", "FK_TENANT_ID" }) })
public class Catalog implements HasPid, Serializable, HasAuditingFields {

    public static final DataFeedTask.IngestionBehavior DEFAULT_INGESTION_BEHAVIOR = Upsert;
    private static final long serialVersionUID = -8455242959058500143L;
    private static final String CATALOG_ID_PREFIX = "ctl_";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("catalog_id")
    @Column(name = "CATALOG_ID", nullable = false)
    private String catalogId;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("primary_key_column")
    @Column(name = "PRIMARY_KEY_COLUMN")
    private String primaryKeyColumn; // column name used to uniquely identify catalog entry

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

    public String getCatalogId() {
        return catalogId;
    }

    public void setCatalogId(String catalogId) {
        this.catalogId = catalogId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public void setPrimaryKeyColumn(String primaryKeyColumn) {
        this.primaryKeyColumn = primaryKeyColumn;
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

    @JsonProperty("task_unique_id")
    public void setDataFeedTaskUniqueId(String uniqueId) {
        if (dataFeedTask == null) {
            // dummy object
            dataFeedTask = new DataFeedTask();
        }
        dataFeedTask.setUniqueId(uniqueId);
    }

    @JsonProperty("task_unique_id")
    public String getDataFeedTaskUniqueId() {
        return dataFeedTask == null ? null : dataFeedTask.getUniqueId();
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

    // check PID only for now
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Catalog catalog = (Catalog) o;
        return Objects.equal(pid, catalog.pid);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pid);
    }

    public static String generateId() {
        String uuid;
        do {
            // try until uuid does not start with catalog prefix
            uuid = AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID()));
        } while (uuid.startsWith(CATALOG_ID_PREFIX));
        return CATALOG_ID_PREFIX + uuid;
    }
}
