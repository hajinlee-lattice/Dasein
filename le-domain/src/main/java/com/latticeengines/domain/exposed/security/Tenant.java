package com.latticeengines.domain.exposed.security;

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.pls.TargetMarket;

@Entity
@Access(AccessType.FIELD)
@Table(name = "TENANT")
public class Tenant implements HasName, HasId<String>, HasPid {

    @Column(name = "TENANT_ID", nullable = false, unique = true)
    private String id;

    @Column(name = "NAME", nullable = false, unique = true)
    private String name;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "TENANT_PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "REGISTERED_TIME", nullable = false)
    private Long registeredTime;

    @Column(name = "UI_VERSION", nullable = false, unique = false)
    private String uiVersion = "2.0";

    @OneToMany(cascade = CascadeType.REMOVE, fetch = FetchType.LAZY, mappedBy = "tenant")
    private List<TargetMarket> targetMarkets;

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public Tenant() {
    }

    public Tenant(String id) {
        setId(id);
    }

    @Override
    @JsonProperty("DisplayName")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("DisplayName")
    public void setName(String name) {
        this.name = name;
    }

    @Override
    @JsonProperty("Identifier")
    public String getId() {
        return id;
    }

    @Override
    @JsonProperty("Identifier")
    public void setId(String id) {
        this.id = id;
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonIgnore
    public Long getRegisteredTime() {
        return registeredTime;
    }

    public void setRegisteredTime(Long registeredTime) {
        this.registeredTime = registeredTime;
    }

    @MetricTag(tag = "TenantId")
    @JsonIgnore
    private String tenantId() {
        return getId();
    }

    @JsonProperty("UIVersion")
    public String getUiVersion() {
        return uiVersion;
    }

    @JsonProperty("UIVersion")
    public void setUiVersion(String uiVersion) {
        this.uiVersion = uiVersion;
    }

    // TODO: Note - this is a terrible hack to avoid DP-2243
    @JsonIgnore
    public List<TargetMarket> getTargetMarkets() {
        return targetMarkets;
    }

    @JsonIgnore
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
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

}
