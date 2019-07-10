package com.latticeengines.domain.exposed.metadata;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.pls.SoftDeletable;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

import io.swagger.annotations.ApiModelProperty;

@Entity
@javax.persistence.Table(name = "METADATA_SEGMENT", uniqueConstraints = @UniqueConstraint(columnNames = { "TENANT_ID",
        "NAME" }))
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Filters({ @Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId"),
        @Filter(name = "softDeleteFilter", condition = "DELETED != true") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MetadataSegment implements HasName, HasPid, HasAuditingFields, HasTenantId, Cloneable, SoftDeletable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
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

    @JsonProperty("created_by")
    @Column(name = "CREATED_BY")
    private String createdBy;

    @JsonProperty("updated_by")
    @Column(name = "UPDATED_BY")
    private String updatedBy;

    @Column(name = "RESTRICTION")
    @Type(type = "text")
    private String restrictionString;

    @Column(name = "CONTACT_RESTRICTION")
    @Type(type = "text")
    private String contactRestrictionString;

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

    @Column(name = "DELETED")
    @JsonProperty("deleted")
    private Boolean deleted = false;

    @JsonProperty("is_master_segment")
    @Column(name = "IS_MASTER_SEGMENT", nullable = false)
    private Boolean isMasterSegment = false;

    @Column(name = "TENANT_ID", nullable = false)
    @JsonIgnore
    private Long tenantId;

    @Column(name = "ACCOUNTS")
    @JsonProperty("accounts")
    private Long accounts;

    @Column(name = "CONTACTS")
    @JsonProperty("contacts")
    private Long contacts;

    @Column(name = "PRODUCTS")
    @JsonProperty("products")
    private Long products;

    @JsonProperty("segment_attributes")
    @Transient
    @ApiModelProperty("segment attributes")
    private Set<AttributeLookup> segmentAttributes;

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

    @JsonProperty("account_restriction")
    @ApiModelProperty("Account restriction for use in the front end")
    public FrontEndRestriction getAccountFrontEndRestriction() {
        return new FrontEndRestriction(getAccountRestriction());
    }

    @JsonProperty("account_restriction")
    @ApiModelProperty("Account restriction for use in the front end")
    public void setAccountFrontEndRestriction(FrontEndRestriction accountFrontEndRestriction) {
        if (accountFrontEndRestriction != null) {
            setAccountRestriction(accountFrontEndRestriction.getRestriction());
        }
    }

    @JsonProperty("contact_restriction")
    @ApiModelProperty("Contact restriction for use in the front end")
    public FrontEndRestriction getContactFrontEndRestriction() {
        return new FrontEndRestriction(getContactRestriction());
    }

    @JsonProperty("contact_restriction")
    @ApiModelProperty("Contact restriction for use in the front end")
    public void setContactFrontEndRestriction(FrontEndRestriction contactFrontEndRestriction) {
        if (contactFrontEndRestriction != null) {
            this.setContactRestriction(contactFrontEndRestriction.getRestriction());
        }
    }

    public Set<AttributeLookup> getSegmentAttributes() {
        return segmentAttributes;
    }

    public void setSegmentAttributes(Set<AttributeLookup> segmentAttributes) {
        this.segmentAttributes = segmentAttributes;
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
    public Boolean getDeleted() {
        return this.deleted;
    }

    @Override
    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
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

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public Long getAccounts() {
        return accounts;
    }

    public void setAccounts(Long accounts) {
        this.accounts = accounts;
    }

    public Long getContacts() {
        return contacts;
    }

    public void setContacts(Long contacts) {
        this.contacts = contacts;
    }

    public Long getProducts() {
        return products;
    }

    public void setProducts(Long products) {
        this.products = products;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public void setEntityCount(BusinessEntity entity, Long count) {
        switch (entity) {
        case Account:
            setAccounts(count);
            break;
        case Contact:
            setContacts(count);
            break;
        case Product:
            setProducts(count);
            break;
        default:
            throw new UnsupportedOperationException("Did not reserve a column for " + entity + " count.");
        }
    }

    public Long getEntityCount(BusinessEntity entity) {
        switch (entity) {
        case Account:
            return getAccounts();
        case Contact:
            return getContacts();
        case Product:
            return getProducts();
        default:
            throw new UnsupportedOperationException("Did not reserve a column for " + entity + " count.");
        }
    }

    public Map<BusinessEntity, Long> getEntityCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        for (BusinessEntity entity : BusinessEntity.COUNT_ENTITIES) {
            Long count = getEntityCount(entity);
            if (count != null) {
                map.put(entity, count);
            }
        }
        return map;
    }

    public FrontEndQuery toFrontEndQuery(BusinessEntity mainEntity) {
        return toRatingEngineFrontEndQuery(mainEntity);
    }

    public RatingEngineFrontEndQuery toRatingEngineFrontEndQuery(BusinessEntity mainEntity) {
        RatingEngineFrontEndQuery frontEndQuery = new RatingEngineFrontEndQuery();
        frontEndQuery.setMainEntity(mainEntity);

        Restriction innerAccountRestriction = getAccountRestriction() == null
                ? getAccountFrontEndRestriction().getRestriction() : getAccountRestriction();
        if (!BusinessEntity.Account.equals(mainEntity)) {
            if (innerAccountRestriction != null) {
                innerAccountRestriction = Restriction.builder().and(innerAccountRestriction, accountNotNullBucket())
                        .build();
            } else {
                innerAccountRestriction = accountNotNullBucket();
            }
        }

        FrontEndRestriction accountRestriction = new FrontEndRestriction(innerAccountRestriction);
        FrontEndRestriction contactRestriction = getContactRestriction() == null ? getContactFrontEndRestriction()
                : new FrontEndRestriction(getContactRestriction());

        frontEndQuery.setAccountRestriction(accountRestriction);
        frontEndQuery.setContactRestriction(contactRestriction);
        return frontEndQuery;
    }

    private BucketRestriction accountNotNullBucket() {
        Bucket bkt = Bucket.notNullBkt();
        return new BucketRestriction(BusinessEntity.Account, InterfaceName.AccountId.name(), bkt);
    }

    @Override
    public Object clone() {
        MetadataSegment clone = new MetadataSegment();
        clone.setCreated(new Date());
        clone.setCreatedBy(this.getCreatedBy());
        clone.setUpdatedBy(this.getUpdatedBy());
        clone.setMasterSegment(this.getMasterSegment());
        clone.setDataCollection(this.getDataCollection());
        clone.setAccountFrontEndRestriction(this.getAccountFrontEndRestriction());
        clone.setAccountRestriction(this.getAccountRestriction());
        clone.setContactFrontEndRestriction(this.getContactFrontEndRestriction());
        clone.setContactRestriction(this.getContactRestriction());
        clone.setDisplayName(this.getDisplayName() + "_COPY");
        clone.setDescription(this.getDescription());
        clone.setUpdated(new Date());
        clone.setMasterSegment(this.getMasterSegment());
        return clone;
    }

}
