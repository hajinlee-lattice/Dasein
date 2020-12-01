package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LookupIdMapConfigValuesLookup;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "LOOKUP_ID_MAP", //
        indexes = { @Index(name = "LOOKUP_ID_MAP_CONFIG_ID", columnList = "ID") }, //
        uniqueConstraints = { @UniqueConstraint(columnNames = { "ORG_ID", "EXT_SYS_TYPE", "FK_TENANT_ID" }) })
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class LookupIdMap implements HasPid, HasId<String>, HasTenant, HasAuditingFields {

    @JsonIgnore
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("configId")
    @Column(name = "ID", unique = true, nullable = false)
    private String id;

    @JsonProperty(CDLConstants.ORG_ID)
    @Column(name = "ORG_ID", nullable = false)
    private String orgId;

    @JsonProperty(CDLConstants.ORG_NAME)
    @Column(name = "ORG_NAME", nullable = false)
    private String orgName;

    @JsonProperty(CDLConstants.EXTERNAL_SYSTEM_TYPE)
    @Column(name = "EXT_SYS_TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    private CDLExternalSystemType externalSystemType;

    @JsonProperty(CDLConstants.EXTERNAL_SYSTEM_NAME)
    @Column(name = "EXT_SYS_NAME", nullable = false)
    @Enumerated(EnumType.STRING)
    private CDLExternalSystemName externalSystemName;

    @JsonProperty("accountId")
    @Column(name = "ACCOUNT_ID")
    private String accountId;

    @JsonProperty("contactId")
    @Column(name = "CONTACT_ID")
    private String contactId;

    @JsonProperty("prospectOwner")
    @Column(name = "PROSPECT_OWNER")
    private String prospectOwner;

    @JsonProperty("description")
    @Column(name = "DESCRIPTION")
    private String description;

    @Type(type = "json")
    @JsonProperty("configValues")
    @Column(name = "CONFIG_VALUES", columnDefinition = "'JSON'")
    private Map<String, String> configValues;

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @JsonProperty("isRegistered")
    @Column(name = "IS_REGISTERED", nullable = false)
    private Boolean isRegistered;

    @JsonProperty("externalAuthentication")
    @OneToOne(mappedBy = "lookupIdMap", fetch = FetchType.EAGER)
    private ExternalSystemAuthentication externalAuthentication;

    @JsonProperty("exportFieldMappings")
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER, mappedBy = "lookupIdMap")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<ExportFieldMetadataMapping> exportFieldMetadataMappings;

    @JsonProperty("exportFolder")
    @Transient
    private String exportFolder;

    @Override
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public Tenant getTenant() {
        return this.tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @Override
    public Date getCreated() {
        return this.created;
    }

    @Override
    public void setCreated(Date time) {
        this.created = time;
    }

    @Override
    public Date getUpdated() {
        return this.updated;
    }

    @Override
    public void setUpdated(Date time) {
        this.updated = time;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getOrgName() {
        return orgName;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public CDLExternalSystemType getExternalSystemType() {
        return externalSystemType;
    }

    public void setExternalSystemType(CDLExternalSystemType externalSystemType) {
        this.externalSystemType = externalSystemType;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getContactId() {
        return contactId;
    }

    public void setContactId(String contactId) {
        this.contactId = contactId;
    }

    public String getProspectOwner() {
        return prospectOwner;
    }

    public void setProspectOwner(String prospectOwner) {
        this.prospectOwner = prospectOwner;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, String> getConfigValues() {
        if (configValues == null) {
            return new HashMap<String, String>();
        }

        return configValues;
    }

    public void setConfigValues(Map<String, String> configValues) {
        this.configValues = configValues;
    }

    public Boolean getIsRegistered() {
        return isRegistered;
    }

    public void setIsRegistered(Boolean isRegistered) {
        this.isRegistered = isRegistered;
    }

    public CDLExternalSystemName getExternalSystemName() {
        return externalSystemName;
    }

    public void setExternalSystemName(CDLExternalSystemName externalSystemName) {
        this.externalSystemName = externalSystemName;
    }

    public ExternalSystemAuthentication getExternalAuthentication() {
        return externalAuthentication;
    }

    public void setExternalAuthentication(ExternalSystemAuthentication externalAuthentication) {
        this.externalAuthentication = externalAuthentication;
    }

    public List<ExportFieldMetadataMapping> getExportFieldMetadataMappings() {
        return exportFieldMetadataMappings;
    }

    public void setExportFieldMappings(List<ExportFieldMetadataMapping> exportFieldMetadataMappings) {
        this.exportFieldMetadataMappings = exportFieldMetadataMappings;
    }

    public String getExportFolder() {
        return exportFolder;
    }

    public void setExportFolder(String exportFolder) {
        this.exportFolder = exportFolder;
    }

    public boolean isTrayEnabled() {
        if (CDLExternalSystemName.LIVERAMP.contains(externalSystemName)) {
            return true;
        }
        return externalAuthentication != null && !StringUtils.isBlank(externalAuthentication.getTrayAuthenticationId())
                && externalAuthentication.getTrayWorkflowEnabled();
    }

    public String getEndDestinationId() {
        if (LookupIdMapConfigValuesLookup.containsEndDestinationIdKey(externalSystemName)) {
            String configValueKey = LookupIdMapConfigValuesLookup.getEndDestinationIdKey(externalSystemName);

            return getConfigValues().get(configValueKey);
        }

        return orgId;
    }

    public boolean isFileSystem() {
        return externalSystemType.equals(CDLExternalSystemType.FILE_SYSTEM);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
