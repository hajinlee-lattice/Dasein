package com.latticeengines.domain.exposed.auth;

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
@Table(name = "GlobalUserTenantRight")
public class GlobalAuthUserTenantRight extends BaseGlobalAuthObject implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "GlobalUserTenantRight_ID", unique = true, nullable = false)
    private Long userTenantRightId;

    @JsonProperty("ga_user")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "User_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private GlobalAuthUser globalAuthUser;

    @JsonProperty("ga_tenant")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "Tenant_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private GlobalAuthTenant globalAuthTenant;

    @JsonProperty("operation_name")
    @Column(name = "Operation_Name", nullable = true)
    private String operationName;

    @JsonProperty("created_by_user")
    @Column(name = "Created_By_User", nullable = true)
    private String createdByUser;

    @Override
    public Long getPid() {
        return userTenantRightId;
    }

    @Override
    public void setPid(Long pid) {
        this.userTenantRightId = pid;
    }

    public GlobalAuthUser getGlobalAuthUser() {
        return globalAuthUser;
    }

    public void setGlobalAuthUser(GlobalAuthUser globalAuthUser) {
        this.globalAuthUser = globalAuthUser;
    }

    public GlobalAuthTenant getGlobalAuthTenant() {
        return globalAuthTenant;
    }

    public void setGlobalAuthTenant(GlobalAuthTenant globalAuthTenant) {
        this.globalAuthTenant = globalAuthTenant;
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    public String getCreatedByUser() { return createdByUser; }

    public void setCreatedByUser(String createdByUser) { this.createdByUser = createdByUser; }
}
