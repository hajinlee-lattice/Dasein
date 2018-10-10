package com.latticeengines.domain.exposed.dante;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
@Table(name = "TalkingPointCache", indexes = {@Index(name = "IX_EXTERNALID", columnList = "External_ID")})
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class DanteTalkingPoint implements Serializable, HasDanteAuditingFields {

    private static final long serialVersionUID = -416386386502430635L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "TalkingPointCache_ID", unique = true, nullable = false)
    private int talkingPointCacheID;

    @Column(name = "External_ID", unique = true, nullable = false, length = 50)
    private String externalID;

    @Column(name = "Play_External_ID", nullable = false, length = 50)
    private String playExternalID;

    @Column(name = "Value", columnDefinition = "VARCHAR(max)")
    private String value;

    @Column(name = "Customer_ID", nullable = false, length = 50)
    private String customerID;

    @Column(name = "Creation_Date", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date creationDate;

    @JsonIgnore
    @Column(name = "Last_Modification_Date", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date lastModificationDate;

    public int getTalkingPointCacheID() {
        return talkingPointCacheID;
    }

    public void setTalkingPointCacheID(int talkingPointCacheID) {
        this.talkingPointCacheID = talkingPointCacheID;
    }

    public String getExternalID() {
        return externalID;
    }

    public void setExternalID(String externalID) {
        this.externalID = externalID;
    }

    public String getPlayExternalID() {
        return playExternalID;
    }

    public void setPlayExternalID(String playExternalID) {
        this.playExternalID = playExternalID;
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
        return (long) talkingPointCacheID;
    }

    public void setPid(Long pid) {
        talkingPointCacheID = pid.intValue();
    }
}
