package com.latticeengines.domain.exposed.security;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

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
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.pls.TargetMarket;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@Entity
@Access(AccessType.FIELD)
@Table(name = "TENANT")
public class Tenant implements HasName, HasId<String>, HasPid, Serializable {
    private static final long serialVersionUID = 3412997313415383107L;

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @JsonProperty("Identifier")
    @Column(name = "TENANT_ID", nullable = false, unique = true)
    private String id;

    @JsonProperty("DisplayName")
    @Column(name = "NAME", nullable = false, unique = true)
    private String name;

    @JsonProperty("Pid")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "TENANT_PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("RegisteredTime")
    @Column(name = "REGISTERED_TIME", nullable = false)
    private Long registeredTime;

    @JsonProperty("UIVersion")
    @Column(name = "UI_VERSION", nullable = false, unique = false)
    private String uiVersion = "2.0";

    @JsonIgnore
    @Column(name = "EXTERNAL_USER_EMAIL_SENT")
    private Boolean emailSent = false;

    @JsonProperty("Status")
    @Column(name = "STATUS")
    private String status;

    @JsonProperty("TenantType")
    @Column(name = "TENANT_TYPE")
    private String tenantType;

    @JsonProperty("Contract")
    @Column(name = "CONTRACT")
    private String contract;

    @JsonIgnore
    @OneToMany(cascade = CascadeType.REMOVE, fetch = FetchType.LAZY, mappedBy = "tenant")
    private List<TargetMarket> targetMarkets;

    public Tenant() {
    }

    public Tenant(String id) {
        setId(id);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public Long getRegisteredTime() {
        return registeredTime;
    }

    public void setRegisteredTime(Long registeredTime) {
        this.registeredTime = registeredTime;
    }

    @MetricField(name = "TenantId", fieldType = MetricField.FieldType.STRING)
    private String tenantId() {
        return getId();
    }

    public String getUiVersion() {
        return uiVersion;
    }

    public void setUiVersion(String uiVersion) {
        this.uiVersion = uiVersion;
    }

    public Boolean getEmailSent() {
        return emailSent;
    }

    public void setEmailSent(Boolean emailSent) {
        this.emailSent = emailSent;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTenantType() {
        return tenantType;
    }

    public void setTenantType(String tenantType) {
        this.tenantType = tenantType;
    }

    public String getContract() {
        return contract;
    }

    public void setContract(String contract) {
        this.contract = contract;
    }

    // TODO: Note - this is a terrible hack to avoid DP-2243
    public List<TargetMarket> getTargetMarkets() {
        return targetMarkets;
    }

    public void setTargetMarkets(List<TargetMarket> targetMarkets) {
        this.targetMarkets = targetMarkets;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Tenant other = (Tenant) obj;
        if (id == null) {
            return other.id == null;
        } else return id.equals(other.id);
    }

}
