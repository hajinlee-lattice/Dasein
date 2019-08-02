package com.latticeengines.domain.exposed.cdl;

import java.util.Date;
import java.util.List;

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
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

import io.swagger.annotations.ApiModelProperty;

@Entity
@Table(name = "ATLAS_EXPORT",
        indexes = { @Index(name = "IX_UUID", columnList = "UUID") },
        uniqueConstraints = {@UniqueConstraint(name = "UX_UUID", columnNames = { "TENANT_ID", "UUID" }) })
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class AtlasExport implements HasPid, HasTenant, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "UUID", nullable = false)
    @JsonProperty("uuid")
    private String uuid;

    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonIgnore
    @JoinColumn(name = "SEGMENT_NAME")
    private String segmentName;

    @JsonProperty("export_type")
    @Column(name = "EXPORT_TYPE")
    @Enumerated(EnumType.STRING)
    private AtlasExportType exportType;

    @JsonProperty("applicationId")
    @Column(name = "APPLICATION_ID")
    private String applicationId;

    @JsonProperty("date_prefix")
    @Column(name = "DATE_PREFIX", length = 15)
    private String datePrefix;

    @JsonProperty("files_under_system_path")
    @Column(name = "FILES_UNDER_SYSTEM_PATH", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<String> filesUnderSystemPath;

    @JsonProperty("files_under_dropfolder")
    @Column(name = "FILES_UNDER_DROPFOLDER", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<String> filesUnderDropFolder;

    @JsonIgnore
    @Column(name = "ACCOUNT_RESTRICTION")
    @Type(type = "text")
    private String accountRestrictionString;

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

    @JsonProperty("created_by")
    @Column(name = "CREATED_BY")
    private String createdBy;

    @JsonProperty("status")
    @Column(name = "STATUS", nullable = false)
    @Enumerated(EnumType.STRING)
    private MetadataSegmentExport.Status status;

    @JsonProperty("path")
    @Column(name = "PATH", length = 2048)
    private String path;

    @JsonProperty("scheduled")
    @Column(name = "SCHEDULED", nullable = false)
    private Boolean scheduled;

    @JsonProperty("cleanup_by")
    @Column(name = "CLEANUP_BY", nullable = false)
    private Date cleanupBy;

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
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public AtlasExportType getExportType() {
        return exportType;
    }

    public void setExportType(AtlasExportType exportType) {
        this.exportType = exportType;
    }

    public String getDatePrefix() {
        return datePrefix;
    }

    public void setDatePrefix(String datePrefix) {
        this.datePrefix = datePrefix;
    }

    public List<String> getFilesUnderSystemPath() {
        return filesUnderSystemPath;
    }

    public void setFilesUnderSystemPath(List<String> filesUnderSystemPath) {
        this.filesUnderSystemPath = filesUnderSystemPath;
    }

    public List<String> getFilesUnderDropFolder() {
        return filesUnderDropFolder;
    }

    public void setFilesUnderDropFolder(List<String> filesUnderDropFolder) {
        this.filesUnderDropFolder = filesUnderDropFolder;
    }

    public FrontEndRestriction getAccountFrontEndRestriction() {
        return StringUtils.isNoneBlank(accountRestrictionString)
                ? JsonUtils.deserialize(accountRestrictionString, FrontEndRestriction.class)
                : new FrontEndRestriction();
    }

    public void setAccountFrontEndRestriction(FrontEndRestriction accountFrontEndRestriction) {
        this.accountFrontEndRestriction = accountFrontEndRestriction;
        this.accountRestrictionString = JsonUtils.serialize(accountFrontEndRestriction);
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

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public MetadataSegmentExport.Status getStatus() {
        return status;
    }

    public void setStatus(MetadataSegmentExport.Status status) {
        this.status = status;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Boolean getScheduled() {
        return scheduled;
    }

    public void setScheduled(Boolean scheduled) {
        this.scheduled = scheduled;
    }

    public Date getCleanupBy() {
        return cleanupBy;
    }

    public void setCleanupBy(Date cleanupBy) {
        this.cleanupBy = cleanupBy;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }
}
