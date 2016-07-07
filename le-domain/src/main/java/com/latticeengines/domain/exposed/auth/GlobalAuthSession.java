package com.latticeengines.domain.exposed.auth;

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
import com.latticeengines.domain.exposed.dataplatform.HasPidTemplated;

@Entity
@Access(AccessType.FIELD)
@Table(name = "GlobalSession")
public class GlobalAuthSession extends BaseGlobalAuthObject implements HasPidTemplated<Integer> {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "GlobalSession_ID", nullable = false)
    private Integer pid;

    @JsonProperty("ticket_id")
    @Column(name = "Ticket_ID", nullable = false)
    private Long ticketId;

    @JsonProperty("user_id")
    @Column(name = "User_ID", nullable = false)
    private Long userId;

    @JsonProperty("tenant_id")
    @Column(name = "Tenant_ID", nullable = false)
    private Long tenantId;

    @Override
    public Integer getPid() {
        return pid;
    }

    @Override
    public void setPid(Integer pid) {
        this.pid = pid;
    }

    public Long getTicketId() {
        return ticketId;
    }

    public void setTicketId(Long ticketId) {
        this.ticketId = ticketId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }
}
