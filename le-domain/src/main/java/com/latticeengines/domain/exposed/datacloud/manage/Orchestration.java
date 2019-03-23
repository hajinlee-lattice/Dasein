package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationConfig;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "Orchestration")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Orchestration implements HasPid, Serializable {

    private static final long serialVersionUID = -8140340078057718702L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "Name", unique = true, nullable = false, length = 100)
    private String name;

    @Column(name = "Config", nullable = false, length = 1000)
    private String configStr;

    @Column(name = "SchedularEnabled", nullable = false)
    private boolean schedularEnabled;

    @Column(name = "MaxRetries", nullable = false)
    private int maxRetries;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "orchestration")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<OrchestrationProgress> progresses;

    @Transient
    private OrchestrationConfig config;

    @Override
    @JsonProperty("PID")
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonProperty("PID")
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("Name")
    public String getName() {
        return name;
    }

    @JsonProperty("Name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("SchedularEnabled")
    public boolean isSchedularEnabled() {
        return schedularEnabled;
    }

    @JsonProperty("SchedularEnabled")
    public void setSchedularEnabled(boolean schedularEnabled) {
        this.schedularEnabled = schedularEnabled;
    }

    @JsonProperty("MaxRetries")
    public int getMaxRetries() {
        return maxRetries;
    }

    @JsonProperty("MaxRetries")
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    @JsonIgnore
    private String getConfigStr() {
        return configStr;
    }

    @JsonIgnore
    public void setConfigStr(String configStr) {
        this.configStr = configStr;
    }

    @JsonIgnore
    public OrchestrationConfig getConfig() {
        if (config == null) {
            config = JsonUtils.deserialize(getConfigStr(), OrchestrationConfig.class);
        }
        return config;
    }

    @JsonIgnore
    public void setConfig(OrchestrationConfig config) {
        this.config = config;
        this.configStr = JsonUtils.serialize(config);
    }

    @JsonIgnore
    public List<OrchestrationProgress> getProgresses() {
        return progresses;
    }

    @JsonIgnore
    public void setProgresses(List<OrchestrationProgress> progresses) {
        this.progresses = progresses;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
