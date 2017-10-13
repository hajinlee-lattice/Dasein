package com.latticeengines.domain.exposed.dante;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
@Table(name = "LeadCache")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class DanteLead implements Serializable, HasDanteAuditingFields {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "LeadCache_ID", unique = true, nullable = false)
    private int leadCacheID;

    @Index(name = "IX_EXTERNALID")
    @Column(name = "External_ID", unique = true, nullable = false, length = 50)
    private String externalID;

    @Column(name = "Salesforce_ID")
    private String salesforceID;

    @Column(name = "Account_External_ID")
    private String accountExternalID;

    @Column(name = "Recommendation_ID")
    private int recommendationID;

    @Column(name = "Value", columnDefinition = "VARCHAR(max)")
    private String value;

    @Column(name = "Customer_ID", nullable = false, length = 50)
    private String customerID;

    @JsonIgnore
    @Column(name = "Creation_Date", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date creationDate;

    @JsonIgnore
    @Column(name = "Last_Modification_Date", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date lastModificationDate;

    public int getLeadCacheID() {
        return leadCacheID;
    }

    public void setLeadCacheID(int leadCacheID) {
        this.leadCacheID = leadCacheID;
    }

    public String getExternalID() {
        return externalID;
    }

    public void setExternalID(String externalID) {
        this.externalID = externalID;
    }

    public String getSalesforceID() {
        return salesforceID;
    }

    public void setSalesforceID(String salesforceID) {
        this.salesforceID = salesforceID;
    }

    public String getAccountExternalID() {
        return accountExternalID;
    }

    public void setAccountExternalID(String accountExternalID) {
        this.accountExternalID = accountExternalID;
    }

    public int getRecommendationID() {
        return recommendationID;
    }

    public void setRecommendationID(int recommendationID) {
        this.recommendationID = recommendationID;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String getCustomerID() {
        return customerID;
    }

    @Override
    public void setCustomerID(String customerID) {
        this.customerID = customerID;
    }

    @Override
    public Date getCreationDate() {
        return creationDate;
    }

    @Override
    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public Date getLastModificationDate() {
        return lastModificationDate;
    }

    @Override
    public void setLastModificationDate(Date lastModificationDate) {
        this.lastModificationDate = lastModificationDate;
    }

    @JsonIgnore
    public Long getPid() {
        return (long) leadCacheID;
    }

    public void setPid(Long pid) {
        leadCacheID = pid.intValue();
    }
}
