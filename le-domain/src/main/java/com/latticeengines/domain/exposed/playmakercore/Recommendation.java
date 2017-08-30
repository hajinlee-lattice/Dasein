package com.latticeengines.domain.exposed.playmakercore;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity
@javax.persistence.Table(name = "Recommendation")
@JsonIgnoreProperties(ignoreUnknown = true)
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class Recommendation implements HasPid, HasId<String>, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Index(name = "REC_EXTERNAL_ID")
    @Column(name = "EXTERNAL_ID", nullable = false)
    @JsonProperty("external_id")
    private String recommendationId;

    @Column(name = "ACCOUNT_ID", nullable = false)
    @JsonProperty("accountId")
    private String accountId;

    @Column(name = "LE_ACCOUNT_EXTERNAL_ID", nullable = false)
    @JsonProperty("leAccountExternalID")
    private String leAccountExternalID;

    @Index(name = "REC_PLAY_ID")
    @Column(name = "PLAY_ID", nullable = false)
    @JsonProperty("play_id")
    private String playId;

    @Index(name = "REC_LAUNCH_ID")
    @Column(name = "LAUNCH_ID", nullable = false)
    @JsonProperty("launch_id")
    private String launchId;

    @Column(name = "DESCRIPTION")
    @JsonProperty("description")
    private String description;

    @Index(name = "REC_LAUNCH_DATE")
    @Column(name = "LAUNCH_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("launchDate")
    private Date launchDate;

    @Index(name = "REC_LAUNCH_LAST_UPD_TIME")
    @Column(name = "LAST_UPDATED_TIMESTAMP", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @JsonProperty("lastUpdatedTimestamp")
    private Date lastUpdatedTimestamp;

    @Column(name = "MONETARY_VALUE")
    @JsonProperty("monetaryValue")
    private Double monetaryValue;

    @Column(name = "LIKELIHOOD")
    @JsonProperty("likelihood")
    private Double likelihood;

    @Column(name = "COMPANY_NAME")
    @JsonProperty("companyName")
    private String companyName;

    @Column(name = "SFDC_ACCOUNT_ID")
    @JsonProperty("sfdcAccountID")
    private String sfdcAccountID;

    @Column(name = "PRIORITY_ID")
    @JsonProperty("priorityID")
    private RuleBucketName priorityID;

    @Column(name = "PRIORITY_DISPLAY_NAME")
    @JsonProperty("priorityDisplayName")
    private String priorityDisplayName;

    @Column(name = "MONETARY_VALUE_ISO4217_ID")
    @JsonProperty("monetaryValueIso4217ID")
    private String monetaryValueIso4217ID;

    @Lob
    @Column(name = "CONTACTS")
    @JsonProperty("CONTACTS")
    private String contacts;

    @Column(name = "SYNC_DESTINATION")
    @JsonProperty("synchronizationDestination")
    private String synchronizationDestination;

    @Index(name = "REC_TENANT_ID")
    @Column(name = "TENANT_ID", nullable = false)
    @JsonProperty("tenant_id")
    private Long tenantId;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getRecommendationId() {
        return recommendationId;
    }

    public void setRecommendationId(String recommendationId) {
        this.recommendationId = recommendationId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getLeAccountExternalID() {
        return leAccountExternalID;
    }

    public void setLeAccountExternalID(String leAccountExternalID) {
        this.leAccountExternalID = leAccountExternalID;
    }

    public String getPlayId() {
        return playId;
    }

    public void setPlayId(String playId) {
        this.playId = playId;
    }

    public String getLaunchId() {
        return launchId;
    }

    public void setLaunchId(String launchId) {
        this.launchId = launchId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getLaunchDate() {
        return launchDate;
    }

    public void setLaunchDate(Date launchDate) {
        this.launchDate = launchDate;
    }

    public Date getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(Date lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    public Double getMonetaryValue() {
        return monetaryValue;
    }

    public void setMonetaryValue(Double monetaryValue) {
        this.monetaryValue = monetaryValue;
    }

    public Double getLikelihood() {
        return likelihood;
    }

    public void setLikelihood(Double likelihood) {
        this.likelihood = likelihood;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getSfdcAccountID() {
        return sfdcAccountID;
    }

    public void setSfdcAccountID(String sfdcAccountID) {
        this.sfdcAccountID = sfdcAccountID;
    }

    public RuleBucketName getPriorityID() {
        return priorityID;
    }

    public void setPriorityID(RuleBucketName priorityID) {
        this.priorityID = priorityID;
    }

    public String getPriorityDisplayName() { return priorityID.getName(); }

    public void setPriorityDisplayName(String name) { priorityDisplayName = name; }

    public String getMonetaryValueIso4217ID() {
        return monetaryValueIso4217ID;
    }

    public void setMonetaryValueIso4217ID(String monetaryValueIso4217ID) {
        this.monetaryValueIso4217ID = monetaryValueIso4217ID;
    }

    public String getContacts() {
        return contacts;
    }

    public void setContacts(String contacts) {
        this.contacts = contacts;
    }

    public List<Map<String, String>> getExpandedContacts() {
        return PlaymakerUtils.getExpandedContacts(this.contacts);
    }

    public void setExpandedContacts(List<Map<String, String>> contactList) {
        this.contacts = JsonUtils.serialize(contactList);
    }

    public String getSynchronizationDestination() {
        return synchronizationDestination;
    }

    public void setSynchronizationDestination(String synchronizationDestination) {
        this.synchronizationDestination = synchronizationDestination;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public String getId() {
        return recommendationId;
    }

    @Override
    public void setId(String id) {
        this.recommendationId = id;
    }
}
