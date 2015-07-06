package com.latticeengines.domain.exposed.oauth;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "OAuthUser", uniqueConstraints = { @UniqueConstraint(columnNames = { "UserId" }) })
public class OAuthUser implements HasPid {

    private Long pid;
    private String userId;
    private String password;
    private Date passwordExpiration;

    public OAuthUser() {
        super();
    }

    public OAuthUser(OAuthUser user) {
        this.userId = user.userId;
        this.password = user.password;
        this.passwordExpiration = user.passwordExpiration;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("UserId")
    @Column(name = "UserId", nullable = false)
    public String getUserId() {
        return userId;
    }

    @JsonProperty("UserId")
    public void setUserId(String userId) {
        this.userId = userId;
    }

    @JsonProperty("Password")
    @Column(name = "Password", nullable = false)
    public String getPassword() {
        return password;
    }

    @JsonProperty("Password")
    public void setPassword(String password) {
        this.password = password;
    }

    @JsonProperty("PasswordExpiration")
    @Column(name = "PasswordExpiration", nullable = false)
    public Date getPasswordExpiration() {
        return passwordExpiration;
    }

    @JsonProperty("PasswordExpiration")
    public void setPasswordExpiration(Date passwordExpiration) {
        this.passwordExpiration = passwordExpiration;
    }

}
