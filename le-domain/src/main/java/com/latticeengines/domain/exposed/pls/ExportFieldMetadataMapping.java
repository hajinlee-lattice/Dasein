package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.NamedQuery;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "EXPORT_FIELD_METADATA_MAPPING")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@NamedQuery(name = ExportFieldMetadataMapping.NQ_FIND_FIELD_MAPPING_BY_LOOKUPMAP_ORG_ID, query = ExportFieldMetadataMapping.SELECT_FIELD_MAPPING_BY_LOOKUPMAP_ORG_ID)
@NamedQuery(name = ExportFieldMetadataMapping.NQ_FIND_ALL_FIELD_MAPPINGS, query = ExportFieldMetadataMapping.SELECT_ALL_FIELD_MAPPINGS)
public class ExportFieldMetadataMapping implements HasPid, HasTenant, HasAuditingFields {

    public static final String NQ_FIND_FIELD_MAPPING_BY_LOOKUPMAP_ORG_ID = "ExportFieldMetadataMapping.findFieldMappingByOrgId";
    public static final String NQ_FIND_ALL_FIELD_MAPPINGS = "ExportFieldMetadataMapping.findAllFieldMappings";
    static final String SELECT_FIELD_MAPPING_BY_LOOKUPMAP_ORG_ID = "FROM ExportFieldMetadataMapping fm WHERE fm.lookupIdMap.orgId = :orgId";
    static final String SELECT_ALL_FIELD_MAPPINGS = "FROM ExportFieldMetadataMapping";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_LOOKUP_ID_MAP", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private LookupIdMap lookupIdMap;

    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonIgnore
    private Tenant tenant;

    @JsonProperty("sourceField")
    @Column(name = "SOURCE_FIELD", nullable = false)
    private String sourceField;

    @JsonProperty("destinationField")
    @Column(name = "DESTINATION_FIELD", nullable = false)
    private String destinationField;

    @JsonProperty("overwriteValue")
    @Column(name = "OVERWRITE_VALUE", nullable = false)
    private Boolean overwriteValue;

    @JsonProperty("createdDate")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updatedDate")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    public ExportFieldMetadataMapping() {

    }

    public ExportFieldMetadataMapping(String sourceField, String destinationField, Boolean overwriteValue) {
        this.sourceField = sourceField;
        this.destinationField = destinationField;
        this.overwriteValue = overwriteValue;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public LookupIdMap getLookupIdMap() {
        return lookupIdMap;
    }

    public void setLookupIdMap(LookupIdMap lookupIdMap) {
        this.lookupIdMap = lookupIdMap;
    }

    @Override
    public Tenant getTenant() {
        // TODO Auto-generated method stub
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getSourceField() {
        return sourceField;
    }

    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }

    public String getDestinationField() {
        return destinationField;
    }

    public void setDestinationField(String destinationField) {
        this.destinationField = destinationField;
    }

    public Boolean getOverwriteValue() {
        return overwriteValue;
    }

    public void setOverwriteValue(Boolean overwriteValue) {
        this.overwriteValue = overwriteValue;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date date) {
        this.created = date;

    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date date) {
        this.updated = date;

    }

}
