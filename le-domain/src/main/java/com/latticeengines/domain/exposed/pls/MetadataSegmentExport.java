package com.latticeengines.domain.exposed.pls;

import java.util.Date;

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
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

import io.swagger.annotations.ApiModelProperty;

@Entity
@javax.persistence.Table(name = "METADATA_SEGMENT_EXPORT", uniqueConstraints = {
        @UniqueConstraint(columnNames = { "EXPORT_ID" }) })
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class MetadataSegmentExport implements HasPid, HasTenantId, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("exportId")
    @Index(name = "METADATA_SEGMENT_EXPORT_ID")
    @Column(name = "EXPORT_ID", nullable = false)
    private String exportId;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_SEGMENT_ID", nullable = true)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private MetadataSegment segment;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("created_by")
    @Column(name = "CREATED_BY")
    private String createdBy;

    @JsonIgnore
    @Column(name = "RESTRICTION")
    @Type(type = "text")
    private String restrictionString;

    @JsonIgnore
    @Column(name = "CONTACT_RESTRICTION")
    @Type(type = "text")
    private String contactRestrictionString;

    @JsonProperty("account_restriction")
    @Transient
    @ApiModelProperty("Account restriction for use in the front end")
    private FrontEndRestriction accountFrontEndRestriction;

    @JsonProperty("contact_restriction")
    @Transient
    @ApiModelProperty("Contact restriction for use in the front end")
    private FrontEndRestriction contactFrontEndRestriction;

    @JsonProperty("applicationId")
    @Column(name = "APPLICATION_ID", nullable = true)
    private String applicationId;

    @JsonProperty("type")
    @Column(name = "TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    private ExportType type;

    @JsonProperty("status")
    @Column(name = "STATUS", nullable = false)
    @Enumerated(EnumType.STRING)
    private Status status;

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    private Date updated;

    @JsonProperty("cleanupBy")
    @Index(name = "METADATA_SEGMENT_EXPORT_TTL")
    @Column(name = "CLEANUP_BY", nullable = false)
    private Date cleanupBy;

    @JsonProperty("path")
    @Column(name = "PATH", nullable = false)
    private String path;

    @JsonProperty("fileName")
    @Column(name = "FILE_NAME", nullable = false)
    private String fileName;

    @JsonProperty("tableName")
    @Column(name = "TABLE_NAME", nullable = false)
    private String tableName;

    @Column(name = "TENANT_ID", nullable = false)
    @JsonIgnore
    private Long tenantId;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getExportId() {
        return exportId;
    }

    public void setExportId(String exportId) {
        this.exportId = exportId;
    }

    public MetadataSegment getSegment() {
        return segment;
    }

    public void setSegment(MetadataSegment segment) {
        this.segment = segment;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public FrontEndRestriction getAccountFrontEndRestriction() {
        return StringUtils.isNoneBlank(restrictionString)
                ? JsonUtils.deserialize(restrictionString, FrontEndRestriction.class) : new FrontEndRestriction();
    }

    public void setAccountFrontEndRestriction(FrontEndRestriction accountFrontEndRestriction) {
        this.accountFrontEndRestriction = accountFrontEndRestriction;
        this.restrictionString = JsonUtils.serialize(accountFrontEndRestriction);
    }

    public FrontEndRestriction getContactFrontEndRestriction() {
        return StringUtils.isNoneBlank(contactRestrictionString)
                ? JsonUtils.deserialize(contactRestrictionString, FrontEndRestriction.class)
                : new FrontEndRestriction();
    }

    public void setContactFrontEndRestriction(FrontEndRestriction contactFrontEndRestriction) {
        this.contactFrontEndRestriction = contactFrontEndRestriction;
        this.contactRestrictionString = JsonUtils.serialize(contactFrontEndRestriction);
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    public Date getCleanupBy() {
        return cleanupBy;
    }

    public void setCleanupBy(Date cleanupBy) {
        this.cleanupBy = cleanupBy;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public ExportType getType() {
        return type;
    }

    public void setType(ExportType type) {
        this.type = type;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public enum ExportType {
        ACCOUNT, //
        CONTACT, //
        ACCOUNT_AND_CONTACT;
    }

    public enum Status {
        RUNNING, //
        FAILED, //
        COMPLETED;
    }
}
