package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;

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

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "METADATA_DATA_COLLECTION_ARTIFACT")
@Filters(value = { @Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId") })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@TypeDefs(value = { @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class DataCollectionArtifact implements HasPid, HasTenant, Serializable {
    private static final long serialVersionUID = 3886398415278083037L;

    //=========================
    // BEGIN: Artifact Types
    //=========================
    // not using enum, for the possibility of weak typing it in future
    public static final String UNMATCHED_ACCOUNTS = "UnmatchedAccount";
    public static final String ORPHAN_CONTACTS = "OrphanContacts";
    public static final String ORPHAN_TRXNS = "OrphanTransactions";
    public static final String FULL_PROFILE = "FullProfile";
    //=========================
    // END: Artifact Types
    //=========================

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @JsonProperty("ArtifactId")
    private Long pid;

    @Column(name = "CREATE_TIME", nullable = false)
    @JsonProperty("CreateTime")
    private Long createTime;

    @Column(name = "NAME")
    @JsonProperty("ArtifactName")
    private String name;

    @Column(name = "URL")
    @JsonProperty("ArtifactURL")
    private String url;

    @Enumerated(EnumType.STRING)
    @Column(name = "STATUS")
    @JsonProperty("ArtifactStatus")
    private Status status;

    @Enumerated(EnumType.STRING)
    @Column(name = "VERSION", nullable = false)
    @JsonProperty("ArtifactVersion")
    private DataCollection.Version version;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_COLLECTION_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private DataCollection dataCollection;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private Tenant tenant;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }

    public DataCollection getDataCollection() {
        return dataCollection;
    }

    public void setDataCollection(DataCollection dataCollection) {
        this.dataCollection = dataCollection;
    }

    public enum Status {
        NOT_SET, GENERATING, READY, STALE;

        public boolean isTerminal() {
            return !GENERATING.equals(this);
        }
    }
}
