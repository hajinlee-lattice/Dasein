package com.latticeengines.documentdb.entity;

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
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "ServiceConfiguration")
@JsonInclude(JsonInclude.Include.NON_NULL)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class ServiceConfigEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @JsonIgnore
    private Long pid;

    @JsonProperty("TenantPId")
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    @JoinColumn(name = "TenantPId", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private TenantConfigEntity tenantConfigEntity;

    @JsonProperty("ServiceName")
    @Column(name = "ServiceName")
    private String serviceName;

    @Type(type = "json")
    @JsonProperty("ServiceConfig")
    @Column(name = "ServiceConfig", columnDefinition = "'JSON'")
    private SerializableDocumentDirectory serviceConfig;

    @Type(type = "json")
    @JsonProperty("State")
    @Column(name = "State", columnDefinition = "'JSON'")
    private BootstrapState state;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public TenantConfigEntity getTenantConfigEntity() {
        return tenantConfigEntity;
    }

    public void setTenantConfigEntity(TenantConfigEntity tenantConfigEntity) {
        this.tenantConfigEntity = tenantConfigEntity;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public SerializableDocumentDirectory getServiceConfig() {
        return serviceConfig;
    }

    public void setServiceConfig(SerializableDocumentDirectory serviceConfig) {
        this.serviceConfig = serviceConfig;
    }

    public BootstrapState getState() {
        return state;
    }

    public void setState(BootstrapState state) {
        this.state = state;
    }
}
