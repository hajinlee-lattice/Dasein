package com.latticeengines.domain.exposed.security;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "TENANT")
public class Tenant implements HasName, HasId<String>, HasPid {

    private String id;
    private String name;
    private Long pid;
    private Long registeredTime;

    @Override
    @JsonProperty("DisplayName")
    @Column(name = "NAME", nullable = false, unique = true)
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
    @Column(name = "TENANT_ID", nullable = false, unique = true)
    public String getId() {
        return id;
    }

    @Override
    @JsonProperty("Identifier")
    public void setId(String id) {
        this.id = id;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "TENANT_PID", unique = true, nullable = false)
    @Override
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
    @Column(name = "REGISTERED_TIME", nullable = false)
    public Long getRegisteredTime() {
        return registeredTime;
    }

    public void setRegisteredTime(Long registeredTime) {
        this.registeredTime = registeredTime;
    }
    
}
