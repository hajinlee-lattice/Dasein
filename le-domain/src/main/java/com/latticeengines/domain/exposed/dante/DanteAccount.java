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
@Table(name = "AccountCache")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class DanteAccount implements Serializable, HasDanteAuditingFields {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "AccountCache_ID", unique = true, nullable = false)
    private int accountCacheID;

    @Index(name = "IX_EXTERNALID")
    @Column(name = "External_ID", unique = true, nullable = false, length = 50)
    private String externalID;

    @JsonIgnore
    @Column(name = "IsSegment", nullable = false)
    private boolean isSegment;

    @JsonIgnore
    @Column(name = "RepresentativeAccounts", nullable = false)
    private long representativeAccounts;

    @Column(name = "LEAccount_External_ID", length = 50)
    private String lEAccountExternalID;

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

    public int getAccountCacheID() {
        return accountCacheID;
    }

    public void setAccountCacheID(int accountCacheID) {
        this.accountCacheID = accountCacheID;
    }

    public String getExternalID() {
        return externalID;
    }

    public void setExternalID(String externalID) {
        this.externalID = externalID;
    }

    public boolean isSegment() {
        return isSegment;
    }

    public void setSegment(boolean segment) {
        isSegment = segment;
    }

    public long getRepresentativeAccounts() {
        return representativeAccounts;
    }

    public void setRepresentativeAccounts(long representativeAccounts) {
        this.representativeAccounts = representativeAccounts;
    }

    public String getlEAccountExternalID() {
        return lEAccountExternalID;
    }

    public void setlEAccountExternalID(String lEAccountExternalID) {
        this.lEAccountExternalID = lEAccountExternalID;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getCustomerID() {
        return customerID;
    }

    public void setCustomerID(String customerID) {
        this.customerID = customerID;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public Date getLastModificationDate() {
        return lastModificationDate;
    }

    public void setLastModificationDate(Date lastModificationDate) {
        this.lastModificationDate = lastModificationDate;
    }

    @JsonIgnore
    public Long getPid() {
        return (long) accountCacheID;
    }

    public void setPid(Long pid) {
        accountCacheID = pid.intValue();
    }
}
