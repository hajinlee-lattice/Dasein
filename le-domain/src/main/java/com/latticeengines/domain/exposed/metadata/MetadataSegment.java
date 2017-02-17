package com.latticeengines.domain.exposed.metadata;

import java.util.Date;

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
import javax.persistence.Transient;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.frontend.query.FrontEndFilter;

@Entity
@javax.persistence.Table(name = "METADATA_SEGMENT")
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataSegment implements HasName, HasPid, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonIgnore
    @Column(name = "RESTRICTION")
    @Type(type = "text")
    private String restrictionString;

    @JsonProperty("front_end_filter")
    @Transient
    private FrontEndFilter frontEndFilter;

    @JsonIgnore
    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_QUERY_SOURCE_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DataCollection dataCollection;

    @Column(name = "UPDATED", nullable = false)
    @JsonProperty("updated")
    private Date updated;

    @Column(name = "CREATED", nullable = false)
    @JsonProperty("created")
    private Date created;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("restriction")
    public Restriction getRestriction() {
        return JsonUtils.deserialize(restrictionString, Restriction.class);
    }

    @JsonProperty("restriction")
    public void setRestriction(Restriction restriction) {
        this.restrictionString = JsonUtils.serialize(restriction);
    }

    public FrontEndFilter getFrontEndFilter() {
        return frontEndFilter;
    }

    public void setFrontEndFilter(FrontEndFilter frontEndFilter) {
        this.frontEndFilter = frontEndFilter;
    }

    public DataCollection getDataCollection() {
        return dataCollection;
    }

    public void setDataCollection(DataCollection dataCollection) {
        this.dataCollection = dataCollection;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
    }
}
