package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

/**
 * Entity class to represent one patch item for DataCloud patcher
 */
@Entity
@Access(AccessType.FIELD)
@Table(name = "PatchBook")
@JsonIgnoreProperties(ignoreUnknown = true)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class PatchBook implements HasPid, Serializable {

    public static final String COLUMN_PID = "PID";
    public static final String COLUMN_TYPE = "Type";
    public static final String COLUMN_HOTFIX = "HotFix";
    public static final String COLUMN_EOL = "EOL";
    public static final String COLUMN_EFFECTIVE_SINCE_VERSION = "EffectiveSinceVersion";
    public static final String COLUMN_EXPIRE_AFTER_VERSION = "ExpireAfterVersion";
    public static final String COLUMN_DOMAIN = "Domain";
    public static final String COLUMN_DUNS = "DUNS";
    public static final String COLUMN_PATCH_ITEMS = "PatchItems";
    public static final String COLUMN_CLEANUP = "Cleanup";
    private static final long serialVersionUID = 5460817063985318621L;
    @Id
    @JsonIgnore
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = COLUMN_PID, unique = true, nullable = false)
    private Long pid;
    @JsonProperty(COLUMN_TYPE)
    @Enumerated(EnumType.STRING)
    @Column(name = COLUMN_TYPE, nullable = false)
    private Type type;
    @JsonProperty(COLUMN_DOMAIN)
    @Column(name = COLUMN_DOMAIN)
    private String domain;

    /* input MatchKey fields */
    // NOTE Column/JSON key name matches MatchKey enum for consistency
    @JsonProperty(COLUMN_DUNS)
    @Column(name = COLUMN_DUNS)
    private String duns;
    @JsonProperty("Name")
    @Column(name = "Name")
    private String name;
    @JsonProperty("Country")
    @Column(name = "Country")
    private String country;
    @JsonProperty("State")
    @Column(name = "State")
    private String state;
    @JsonProperty("City")
    @Column(name = "City")
    private String city;
    @JsonProperty("Zipcode")
    @Column(name = "Zipcode")
    private String zipcode;
    @JsonProperty(COLUMN_PATCH_ITEMS)
    @Column(name = COLUMN_PATCH_ITEMS, columnDefinition = "'JSON'")
    @org.hibernate.annotations.Type(type = "json")
    private Map<String, Object> patchItems;

    /* input MatchKey fields */
    @JsonProperty(COLUMN_CLEANUP)
    @Column(name = COLUMN_CLEANUP, nullable = false)
    private boolean cleanup;
    @JsonProperty(COLUMN_HOTFIX)
    @Column(name = COLUMN_HOTFIX, nullable = false)
    private boolean hotFix;
    @JsonProperty(COLUMN_EOL)
    @Column(name = COLUMN_EOL, nullable = false)
    private boolean endOfLife;
    @JsonProperty("EffectiveSince")
    @Column(name = "EffectiveSince")
    private Date effectiveSince;
    @JsonProperty("ExpireAfter")
    @Column(name = "ExpireAfter")
    private Date expireAfter;
    @JsonProperty("CreatedDate")
    @Column(name = "CreatedDate")
    private Date createdDate;
    @JsonProperty("CreatedBy")
    @Column(name = "CreatedBy")
    private String createdBy;
    @JsonProperty("LastModifiedDate")
    @Column(name = "LastModifiedDate")
    private Date lastModifiedDate;
    @JsonProperty("LastModifiedBy")
    @Column(name = "LastModifiedBy")
    private String lastModifiedBy;
    @JsonProperty("Logs")
    @Column(name = "Logs")
    private String logs;
    @JsonProperty(COLUMN_EFFECTIVE_SINCE_VERSION)
    @Column(name = COLUMN_EFFECTIVE_SINCE_VERSION)
    private String effectiveSinceVersion;
    @JsonProperty(COLUMN_EXPIRE_AFTER_VERSION)
    @Column(name = COLUMN_EXPIRE_AFTER_VERSION)
    private String expireAfterVersion;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getZipcode() {
        return zipcode;
    }

    public void setZipcode(String zipcode) {
        this.zipcode = zipcode;
    }

    public Map<String, Object> getPatchItems() {
        return patchItems;
    }

    public void setPatchItems(Map<String, Object> patchItems) {
        this.patchItems = patchItems;
    }

    public boolean isCleanup() {
        return cleanup;
    }

    public void setCleanup(boolean cleanup) {
        this.cleanup = cleanup;
    }

    public boolean isHotFix() {
        return hotFix;
    }

    public void setHotFix(boolean hotFix) {
        this.hotFix = hotFix;
    }

    public boolean isEndOfLife() {
        return endOfLife;
    }

    public void setEndOfLife(boolean endOfLife) {
        this.endOfLife = endOfLife;
    }

    public Date getEffectiveSince() {
        return effectiveSince;
    }

    public void setEffectiveSince(Date effectiveSince) {
        this.effectiveSince = effectiveSince;
    }

    public Date getExpireAfter() {
        return expireAfter;
    }

    public void setExpireAfter(Date expireAfter) {
        this.expireAfter = expireAfter;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(Date lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    public String getLastModifiedBy() {
        return lastModifiedBy;
    }

    public void setLastModifiedBy(String lastModifiedBy) {
        this.lastModifiedBy = lastModifiedBy;
    }

    public String getLogs() {
        return logs;
    }

    public void setLogs(String logs) {
        this.logs = logs;
    }

    public String getEffectiveSinceVersion() {
        return effectiveSinceVersion;
    }

    public void setEffectiveSinceVersion(String effectiveSinceVersion) {
        this.effectiveSinceVersion = effectiveSinceVersion;
    }

    public String getExpireAfterVersion() {
        return expireAfterVersion;
    }

    public void setExpireAfterVersion(String expireAfterVersion) {
        this.expireAfterVersion = expireAfterVersion;
    }

    public enum Type {
        Attribute, Lookup, Domain
    }
}
