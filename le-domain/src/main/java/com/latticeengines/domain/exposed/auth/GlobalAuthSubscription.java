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
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "GlobalSubscription")
public class GlobalAuthSubscription extends BaseGlobalAuthObject implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "GlobalSubscription_ID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("ga_user_tenant_right")
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "UserTenantRight_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private GlobalAuthUserTenantRight gaUserTenantRight;

    @JsonProperty("tenant_id")
    @Column(name = "Tenant_ID", nullable = false)
    private String tenantId;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public GlobalAuthUserTenantRight getUserTenantRight() {
        return gaUserTenantRight;
    }

    public void setUserTenantRight(GlobalAuthUserTenantRight gaUserTenantRight) {
        this.gaUserTenantRight = gaUserTenantRight;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
}
