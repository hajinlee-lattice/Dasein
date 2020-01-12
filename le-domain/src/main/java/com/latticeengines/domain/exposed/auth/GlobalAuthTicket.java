package com.latticeengines.domain.exposed.auth;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Access(AccessType.FIELD)
@Table(name = "GlobalTicket", indexes = {
        @Index(name = "IX_Ticket", columnList = "Ticket"),
        @Index(name = "IX_User_Issuer", columnList = "User_ID,Issuer") })
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)})
public class GlobalAuthTicket extends BaseGlobalAuthObject implements HasPid {

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

    @JsonProperty("external_session")
    @Type(type = "json")
    @Column(name = "External_Session", columnDefinition = "'JSON'")
    private GlobalAuthExternalSession externalSession;

    @JsonIgnore
    @Column(name = "Issuer", //
            columnDefinition = "'VARCHAR(255) GENERATED ALWAYS AS (`External_Session` ->> '$.Issuer')'", //
            insertable = false, updatable = false)
    private String issuer;

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

    public GlobalAuthExternalSession getExternalSession() {
        return externalSession;
    }

    public void setExternalSession(GlobalAuthExternalSession externalSession) {
        this.externalSession = externalSession;
    }
}
