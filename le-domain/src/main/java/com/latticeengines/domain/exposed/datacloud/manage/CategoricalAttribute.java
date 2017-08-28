package com.latticeengines.domain.exposed.datacloud.manage;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "CategoricalAttribute")
@JsonIgnoreProperties(ignoreUnknown = true)
public class CategoricalAttribute implements HasPid {

    public static final String ALL = "_ALL_";

    public static final String OTHER = "_OTHER_";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "AttrName", nullable = false, length = 100)
    private String attrName;

    @Column(name = "AttrValue", nullable = false, length = 500)
    private String attrValue;

    @Index(name = "IX_PARENT_ID")
    @Column(name = "ParentID")
    private Long parentId;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public String getAttrValue() {
        return attrValue;
    }

    public void setAttrValue(String attrValue) {
        this.attrValue = attrValue;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }
}
