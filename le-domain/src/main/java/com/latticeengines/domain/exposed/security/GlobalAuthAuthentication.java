package com.latticeengines.domain.exposed.security;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "GlobalAuthentication")
public class GlobalAuthAuthentication implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "GlobalAuthentication_ID", nullable = false)
    private Long authenticationId;

    @JsonProperty("ga_user")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "User_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private GlobalAuthUser globalAuthUser;

    @JsonProperty("username")
    @Column(name = "Username", nullable = false)
    private String username;

    @JsonProperty("password")
    @Column(name = "Password", nullable = false)
    private String password;

    @JsonProperty("must_change_password")
    @Column(name = "MustChangePassword", nullable = false)
    private boolean mustChangePassword;

    @JsonProperty("creation_date")
    @Column(name = "Creation_Date", nullable = false)
    private Date creationDate;

    @JsonProperty("last_modification_date")
    @Column(name = "Last_Modification_Date", nullable = false)
    private Date lastModificationDate;

    public GlobalAuthAuthentication() {
        creationDate = new Date(System.currentTimeMillis());
        lastModificationDate = new Date(System.currentTimeMillis());
    }

    @Override
    public Long getPid() {
        return authenticationId;
    }

    @Override
    public void setPid(Long pid) {
        this.authenticationId = pid;

    }

    public GlobalAuthUser getGlobalAuthUser() {
        return globalAuthUser;
    }

    public void setGlobalAuthUser(GlobalAuthUser globalAuthUser) {
        this.globalAuthUser = globalAuthUser;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean getMustChangePassword() {
        return mustChangePassword;
    }

    public void setMustChangePassword(boolean mustChangePassword) {
        this.mustChangePassword = mustChangePassword;
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
