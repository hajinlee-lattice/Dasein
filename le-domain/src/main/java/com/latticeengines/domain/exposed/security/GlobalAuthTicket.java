package com.latticeengines.domain.exposed.security;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "GlobalTicket")
public class GlobalAuthTicket implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "GlobalTicket_ID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("ticket")
    @Column(name = "Ticket", nullable = false)
    private String ticket;

    @JsonProperty("user_id")
    @Column(name = "User_ID", nullable = false)
    private Long userId;

    @JsonProperty("last_access_date")
    @Column(name = "Last_Access_Date", nullable = false)
    private Date lastAccessDate;

    @JsonProperty("creation_date")
    @Column(name = "Creation_Date", nullable = false)
    private Date creationDate;

    @JsonProperty("last_modification_date")
    @Column(name = "Last_Modification_Date", nullable = false)
    private Date lastModificationDate;

    public GlobalAuthTicket() {
        creationDate = new Date(System.currentTimeMillis());
        lastModificationDate = new Date(System.currentTimeMillis());
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getTicket() {
        return ticket;
    }

    public void setTicket(String ticket) {
        this.ticket = ticket;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Date getLastAccessDate() {
        return lastAccessDate;
    }

    public void setLastAccessDate(Date lastAccessDate) {
        this.lastAccessDate = lastAccessDate;
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
}
