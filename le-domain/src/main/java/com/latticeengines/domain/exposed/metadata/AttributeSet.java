package com.latticeengines.domain.exposed.metadata;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "METADATA_ATTRIBUTE_SET", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"NAME", "FK_TENANT_ID"})})
@Filters({ //
        @Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AttributeSet implements HasPid, HasName, HasTenant, HasAuditingFields {

    private static final Logger log = LoggerFactory.getLogger(AttributeSet.class);

    private static final String ATTRIBUTE_SET_NAME_PREFIX = "attibute_set";
    private static final String ATTRIBUTE_SET_NAME_FORMAT = "%s__%s";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("name")
    private String name;

    @Column(name = "DISPLAY_NAME", nullable = false)
    @JsonProperty("displayName")
    private String displayName;

    @Column(name = "DESCRIPTION")
    @JsonProperty("description")
    private String description;

    @Column(name = "ATTRIBUTES")
    @Lob
    @JsonIgnore
    private byte[] attributes;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    //  Map<String, Set<String>> --> <category --> list of attribute ids>
    @JsonProperty("attributesMap")
    @Transient
    public Map<String, Set<String>> getAttributesMap() {
        if (getAttributes() == null) {
            return new HashMap<>();
        }
        String uncompressedData = new String(CompressionUtils.decompressByteArray(getAttributes()));
        if (StringUtils.isNotEmpty(uncompressedData)) {
            return JsonUtils.deserialize(uncompressedData,
                    new TypeReference<Map<String, Set<String>>>() {
                    });
        } else {
            return new HashMap<>();
        }
    }

    @JsonProperty("attributesMap")
    @Transient
    public void setAttributesMap(Map<String, Set<String>> attributesMap) {
        if (MapUtils.isEmpty(attributesMap)) {
            setAttributes(null);
            return;
        }
        String string = JsonUtils.serialize(attributesMap);
        byte[] payloadData = string.getBytes();
        try {
            byte[] compressedData = CompressionUtils.compressByteArray(payloadData);
            setAttributes(compressedData);
        } catch (IOException e) {
            log.error("Failed to compress payload [" + attributesMap + "]", e);
        }
    }

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @JsonProperty("createdBy")
    @Column(name = "CREATED_BY")
    private String createdBy;

    @JsonProperty("updatedBy")
    @Column(name = "UPDATED_BY")
    private String updatedBy;

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

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public byte[] getAttributes() {
        return attributes;
    }

    public void setAttributes(byte[] attributes) {
        this.attributes = attributes;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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

    public static String generateNameStr() {
        return String.format(ATTRIBUTE_SET_NAME_FORMAT, ATTRIBUTE_SET_NAME_PREFIX,
                AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID())));
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }
}
