package com.latticeengines.domain.exposed.datacloud.manage;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "CategoricalDimension", //
        indexes = { @Index(name = "IX_SOURCE_DIMENSION", columnList = "Source,Dimension") }, //
        uniqueConstraints = { @UniqueConstraint(columnNames = { "Source", "Dimension" }) })
@JsonIgnoreProperties(ignoreUnknown = true)
public class CategoricalDimension implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "Source", nullable = false, length = 100)
    private String source;

    @Column(name = "Dimension", nullable = false, length = 100)
    private String dimension;

    @Column(name = "RootAttrId", nullable = false, unique = true)
    private Long rootAttrId;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public Long getRootAttrId() {
        return rootAttrId;
    }

    public void setRootAttrId(Long rootAttrId) {
        this.rootAttrId = rootAttrId;
    }
}
