package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

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
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@javax.persistence.Table(name = "METADATA_DATA_COLLECTION_STATUS")
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class DataCollectionStatus implements HasPid, HasTenant, HasAuditingFields, Serializable {


    private static final long serialVersionUID = 3180682380551859839L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_COLLECTION_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DataCollection dataCollection;

    @Enumerated(EnumType.STRING)
    @Column(name = "VERSION", nullable = false)
    private DataCollection.Version version;

    @Type(type = "json")
    @Column(name = "Detail", columnDefinition = "'JSON'")
    private DataCollectionStatusDetail detail = new DataCollectionStatusDetail();

    @Column(name = "CREATION_TIME", nullable = false)
    private Date creationTime;

    @Column(name = "UPDATE_TIME", nullable = false)
    private Date updateTime;

    @Override
    @JsonIgnore
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonIgnore
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    @JsonIgnore
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @JsonIgnore
    public DataCollection getDataCollection() {
        return dataCollection;
    }

    @JsonIgnore
    public void setDataCollection(DataCollection dataCollection) {
        this.dataCollection = dataCollection;
    }

    @JsonIgnore
    public DataCollection.Version getVersion() {
        return version;
    }

    @JsonIgnore
    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }

    @JsonIgnore
    public DataCollectionStatusDetail getDetail() {
        return detail;
    }

    @JsonIgnore
    public void setDetail(DataCollectionStatusDetail detail) {
        this.detail = detail;
    }

    @JsonProperty("MinTxnDate")
    public Integer getMinTxnDate() {
        return this.detail.getMinTxnDate();
    }

    @JsonProperty("MinTxnDate")
    public void setMinTxnDate(Integer minTxnDate) {
        this.detail.setMinTxnDate(minTxnDate);
    }

    @JsonProperty("MaxTxnDate")
    public Integer getMaxTxnDate() {
        return this.detail.getMaxTxnDate();
    }

    @JsonProperty("MaxTxnDate")
    public void setMaxTxnDate(Integer maxTxnDate) {
        this.detail.setMaxTxnDate(maxTxnDate);
    }

    @JsonProperty("EvaluationDate")
    public String getEvaluationDate() {
        return this.detail.getEvaluationDate();
    }

    @JsonProperty("EvaluationDate")
    public void setEvaluationDate(String evaluationDate) {
        this.detail.setEvaluationDate(evaluationDate);
    }

    @JsonProperty("DataCloudBuildNumber")
    public String getDataCloudBuildNumber() {
        return this.detail.getDataCloudBuildNumber();
    }

    @JsonProperty("DataCloudBuildNumber")
    public void setDataCloudBuildNumber(String dataCloudBuildNumber) {
        this.detail.setDataCloudBuildNumber(dataCloudBuildNumber);
    }

    @JsonProperty("AccountCount")
    public Long getAccountCount() {
        return this.detail.getAccountCount();
    }

    @JsonProperty("AccountCount")
    public void setAccountCount(Long accountCount) {
        this.detail.setAccountCount(accountCount);
    }

    @JsonProperty("ContactCount")
    public Long getContactCount() {
        return this.detail.getContactCount();
    }

    @JsonProperty("ContactCount")
    public void setContactCount(Long contactCount) {
        this.detail.setContactCount(contactCount);
    }

    @JsonProperty("TransactionCount")
    public Long getTransactionCount() {
        return this.detail.getTransactionCount();
    }

    @JsonProperty("ProductCount")
    public Long getProductCount() {
        return this.detail.getProductCount();
    }

    @JsonProperty("ProductCount")
    public void setProductCount(Long productCount) {
        this.detail.setProductCount(productCount);
    }

    @JsonProperty("OrphanContactCount")
    public Long getOrphanContactCount() {
        return this.detail.getOrphanContactCount();
    }

    @JsonProperty("OrphanContactCount")
    public void setOrphanContactCount(Long orphanContactCount) {
        this.detail.setOrphanContactCount(orphanContactCount);
    }

    @JsonProperty("OrphanTransactionCount")
    public Long getOrphanTransactionCount() {
        return this.detail.getOrphanTransactionCount();
    }

    @JsonProperty("OrphanTransactionCount")
    public void setOrphanTransactionCount(Long orphanTransactionCount) {
        this.detail.setOrphanTransactionCount(orphanTransactionCount);
    }

    @JsonProperty("UnmatchedAccountCount")
    public Long getUnmatchedAccountCount() {
        return this.detail.getUnmatchedAccountCount();
    }

    @JsonProperty("UnmatchedAccountCount")
    public void setUnmatchedAccountCount(Long unmatchedAccountCount) {
        this.detail.setUnmatchedAccountCount(unmatchedAccountCount);
    }

    @JsonProperty("TransactionCount")
    public void setTransactionCount(Long transactionCount) {
        this.detail.setTransactionCount(transactionCount);
    }

    @JsonProperty("ApsRollingPeriod")
    public String getApsRollingPeriod() {
        return this.detail.getApsRollingPeriod();
    }

    @JsonProperty("ApsRollingPeriod")
    public void setApsRollingPeriod(String apsRollupPeriod) {
        this.detail.setApsRollingPeriod(apsRollupPeriod);
    }

    @JsonProperty("DateMap")
    public Map<String, Long> getDateMap() {
        return this.detail.getDateMap();
    }

    @JsonProperty("DateMap")
    public void setDateMap(Map<String, Long> dateMap) {
        this.detail.setDateMap(dateMap);
    }

    @JsonProperty("OrigCatalogFileMap")
    public Map<String, String> getOrigCatalogFileMap() {
        return this.detail.getOrigCatalogFileMap();
    }

    @JsonProperty("OrigCatalogFileMap")
    public void setOrigCatalogFileMap(Map<String, String> origCatalogFileMap) {
        this.detail.setOrigCatalogFileMap(origCatalogFileMap);
    }

    @Override
    @JsonProperty("CreationTime")
    @Temporal(TemporalType.TIMESTAMP)
    public Date getCreated() {
        return creationTime;
    }

    @Override
    @JsonProperty("CreationTime")
    public void setCreated(Date created) {
        this.creationTime = created;
    }

    @Override
    @JsonProperty("UpdateTime")
    @Temporal(TemporalType.TIMESTAMP)
    public Date getUpdated() {
        return updateTime;
    }

    @Override
    @JsonProperty("UpdateTime")
    public void setUpdated(Date updated) {
        this.updateTime = updated;
    }

}
