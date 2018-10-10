package com.latticeengines.domain.exposed.datacloud.match.patch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

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
import java.io.Serializable;
import java.util.Date;

/**
 * Entity class to represent one patch item for DataCloud patcher
 */
@Entity
@Access(AccessType.FIELD)
@Table(name = "PatchBook")
@JsonIgnoreProperties(ignoreUnknown = true)
public class PatchBook implements HasPid, Serializable {

    private static final long serialVersionUID = 5460817063985318621L;

    public enum Type {
        Attribute, Lookup, Domain
    }

    /* TODO add comments for fields */
    /* TODO add index or modify column definition if necessary */

    @Id
    @JsonIgnore
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("Type")
    @Enumerated(EnumType.STRING)
    @Column(name = "Type", nullable = false)
    private Type type;

    @JsonProperty("Domain")
    @Column(name = "Domain")
    private String domain;

    @JsonProperty("DUNS")
    @Column(name = "DUNS")
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

    @JsonProperty("PatchItems")
    @Column(name = "PatchItems")
    private String patchItems;

    @JsonProperty("Cleanup")
    @Column(name = "Cleanup", nullable = false)
    private boolean cleanup;

    @JsonProperty("HotFix")
    @Column(name = "HotFix", nullable = false)
    private boolean hotFix;

    @JsonProperty("EOL")
    @Column(name = "EOL", nullable = false)
    private boolean endOfLife;

    @JsonProperty("EffectiveSince")
    @Column(name = "EffectiveSince", nullable = false)
    private Date effectiveSince;

    @JsonProperty("ExpireAfter")
    @Column(name = "ExpireAfter", nullable = false)
    private Date expireAfter;

    @JsonProperty("CreatedDate")
    @Column(name = "CreatedDate", nullable = false)
    private Date createdDate;

    @JsonProperty("CreatedBy")
    @Column(name = "CreatedBy")
    private String createdBy;

    @JsonProperty("LastModifiedDate")
    @Column(name = "LastModifiedDate", nullable = false)
    private Date lastModifiedDate;

    @JsonProperty("LastModifiedBy")
    @Column(name = "LastModifiedBy")
    private String lastModifiedBy;

    @JsonProperty("Logs")
    @Column(name = "Logs")
    private String logs;

    @JsonProperty("EffectiveSinceVersion")
    @Column(name = "EffectiveSinceVersion")
    private String effectiveSinceVersion;

    @JsonProperty("ExpireAfterVersion")
    @Column(name = "ExpireAfterVersion")
    private String expireAfterVersion;

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

    public String getPatchItems() {
        return patchItems;
    }

    public void setPatchItems(String patchItems) {
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

    public static long getSerialVersionUID() {
        return serialVersionUID;
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
}
