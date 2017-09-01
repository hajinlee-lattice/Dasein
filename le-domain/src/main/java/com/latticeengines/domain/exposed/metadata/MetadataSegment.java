package com.latticeengines.domain.exposed.metadata;

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

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

import io.swagger.annotations.ApiModelProperty;

@Entity
@javax.persistence.Table(name = "METADATA_SEGMENT", uniqueConstraints = @UniqueConstraint(columnNames = { "TENANT_ID",
        "NAME" }))
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MetadataSegment implements HasName, HasPid, HasAuditingFields, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("display_name")
    @Column(name = "DISPLAY_NAME", nullable = false)
    private String displayName;

    @JsonProperty("description")
    @Column(name = "DESCRIPTION", length = 1000)
    private String description;

    @JsonIgnore
    @Column(name = "RESTRICTION", nullable = true)
    @Type(type = "text")
    private String restrictionString;

    @JsonIgnore
    @Column(name = "CONTACT_RESTRICTION", nullable = true)
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

    @Column(name = "ACCOUNT_CONTACT_OPERATOR", nullable = true)
    @JsonProperty("account_contact_operator")
    @Enumerated(EnumType.STRING)
    private LogicalOperator accountContactOperator;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_COLLECTION_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DataCollection dataCollection;

    @Column(name = "UPDATED", nullable = false)
    @JsonProperty("updated")
    private Date updated;

    @Column(name = "CREATED", nullable = false)
    @JsonProperty("created")
    private Date created;

    @JsonProperty("is_master_segment")
    @Column(name = "IS_MASTER_SEGMENT", nullable = false)
    private Boolean isMasterSegment = false;

    @Column(name = "TENANT_ID", nullable = false)
    @JsonIgnore
    private Long tenantId;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @JsonIgnore
    public void setAccountContactOperator(String operatorString) {
        LogicalOperator logicalOperator = null;
        try {
            logicalOperator = LogicalOperator.valueOf(operatorString);
        } catch (Exception e) {
            // pass
        }
        if (logicalOperator != null) {
            setAccountContactOperator(logicalOperator);
        }
    }

    public LogicalOperator getAccountContactOperator() {
        return accountContactOperator;
    }

    public void setAccountContactOperator(LogicalOperator accountContactOperator) {
        this.accountContactOperator = accountContactOperator;
    }

    @JsonProperty("account_raw_restriction")
    public Restriction getAccountRestriction() {
        return JsonUtils.deserialize(restrictionString, Restriction.class);
    }

    @JsonProperty("account_raw_restriction")
    public void setAccountRestriction(Restriction restriction) {
        this.restrictionString = JsonUtils.serialize(restriction);
    }

    @JsonProperty("contact_raw_restriction")
    public Restriction getContactRestriction() {
        return JsonUtils.deserialize(contactRestrictionString, Restriction.class);
    }

    @JsonProperty("contact_raw_restriction")
    public void setContactRestriction(Restriction restriction) {
        this.contactRestrictionString = JsonUtils.serialize(restriction);
    }

    public FrontEndRestriction getAccountFrontEndRestriction() {
        return accountFrontEndRestriction;
    }

    public void setAccountFrontEndRestriction(FrontEndRestriction accountFrontEndRestriction) {
        this.accountFrontEndRestriction = accountFrontEndRestriction;
    }

    public FrontEndRestriction getContactFrontEndRestriction() {
        return contactFrontEndRestriction;
    }

    public void setContactFrontEndRestriction(FrontEndRestriction contactFrontEndRestriction) {
        this.contactFrontEndRestriction = contactFrontEndRestriction;
    }

    public DataCollection getDataCollection() {
        return dataCollection;
    }

    public void setDataCollection(DataCollection dataCollection) {
        this.dataCollection = dataCollection;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    @Override
    @JsonIgnore
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    @JsonIgnore
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @JsonIgnore
    public void setTenant(Tenant tenant) {
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    public Boolean getMasterSegment() {
        return isMasterSegment;
    }

    public void setMasterSegment(Boolean masterSegment) {
        isMasterSegment = masterSegment;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
