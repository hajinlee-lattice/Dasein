package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dataplatform.HasId;


@Entity
@Table(name = "AccountMaster_Attributes")
public class AmAttribute implements HasPid, Serializable {

    private Long pid;
    private String attrKey;
    private String attrValue;
    private String parentKey; 
    private String parentValue;
    private String source;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Override
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    public Long getPid() {
        return this.pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("AttrKey")
    @Column(name = "AttrKey", nullable = false)
    public String getAttrKey() {
        return attrKey;
    }

    @JsonProperty("AttrKey")
    public void setAttrKey(String attrKey) {
        this.attrKey = attrKey;
    }

    @JsonProperty("AttrValue")
    @Column(name = "AttrValue", nullable = false)
    public String getAttrValue() {
        return attrValue;
    }

    public void setAttrValue(String attrValue) {
        this.attrValue = attrValue;
    }

    @JsonProperty("ParentKey")
    @Column(name = "ParentKey", nullable = true)
    public String getParentKey() {
        return parentKey;
    }

    @JsonProperty("ParentKey")
    public void setParentKey(String parentKey) {
        this.parentKey = parentKey;
    }

    @JsonProperty("ParentValue")
    @Column(name = "ParentValue", nullable = true)
    public String getParentValue() {
        return parentValue;
    }

    @JsonProperty("ParentValue")
    public void setParentValue(String parentValue) {
        this.parentValue = parentValue;
    }

    @JsonProperty("Source")
    @Column(name = "Source", nullable = true)
    public String getSource() {
        return source;
    }

    @JsonProperty("Source")
    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
