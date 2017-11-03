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

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.HasTenantId;

import io.swagger.annotations.ApiModelProperty;

@Entity
@javax.persistence.Table(name = "METADATA_SEGMENT_EXPORT")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class MetadataSegmentExport implements HasName, HasPid, HasTenantId, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Index(name = "METADATA_SEGMENT_EXPORT_NAME")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_SEGMENT_ID", nullable = true)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private MetadataSegment segment;

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
    @Index(name = "METADATA_SEGMENT_EXPORT_NAME")
    @Column(name = "CLEANUP_BY", nullable = false)
    private Date cleanupBy;

    @JsonProperty("path")
    @Column(name = "PATH", nullable = false)
    private String path;

    @Column(name = "TENANT_ID", nullable = false)
    @JsonIgnore
    private Long tenantId;

    @Override
    public Long getPid() {
        return pid;
    }

    public String getName() {
        return name;
    }

    public MetadataSegment getSegment() {
        return segment;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public String getRestrictionString() {
        return restrictionString;
    }

    public String getContactRestrictionString() {
        return contactRestrictionString;
    }

    public FrontEndRestriction getAccountFrontEndRestriction() {
        return accountFrontEndRestriction;
    }

    public FrontEndRestriction getContactFrontEndRestriction() {
        return contactFrontEndRestriction;
    }

    public String getApplicationId() {
        return applicationId;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSegment(MetadataSegment segment) {
        this.segment = segment;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public void setRestrictionString(String restrictionString) {
        this.restrictionString = restrictionString;
    }

    public void setContactRestrictionString(String contactRestrictionString) {
        this.contactRestrictionString = contactRestrictionString;
    }

    public void setAccountFrontEndRestriction(FrontEndRestriction accountFrontEndRestriction) {
        this.accountFrontEndRestriction = accountFrontEndRestriction;
    }

    public void setContactFrontEndRestriction(FrontEndRestriction contactFrontEndRestriction) {
        this.contactFrontEndRestriction = contactFrontEndRestriction;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public enum Status {
        RUNNING, //
        FAILED, //
        COMPLETED;
    }
}
