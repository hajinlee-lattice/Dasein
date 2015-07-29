package com.latticeengines.domain.exposed.pls;

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
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "SALESFORCE_URL")
public class SalesforceURL implements HasName, HasPid {

    private Long pid;
    private String name;
    private String url;

    @Override
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

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

    @JsonProperty("DisplayURL")
    @Column(name = "URL", nullable = false)
    public String getURL() {
        return url;
    }

    @JsonProperty("DisplayURL")
    public void setURL(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}