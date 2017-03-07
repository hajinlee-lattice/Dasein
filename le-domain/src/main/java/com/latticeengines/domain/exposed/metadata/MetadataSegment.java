package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import javax.persistence.OneToMany;
import javax.persistence.Transient;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FlattenedRestriction;

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

    @JsonProperty("display_name")
    @Column(name = "DISPLAY_NAME", nullable = false)
    private String displayName;

    @JsonProperty("description")
    @Column(name = "DESCRIPTION", nullable = true)
    private String description;

    @JsonIgnore
    @Column(name = "RESTRICTION")
    @Type(type = "text")
    private String restrictionString;

    @JsonProperty("simple_restriction")
    @Transient
    private FlattenedRestriction simpleRestriction;

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

    @OneToMany(cascade = CascadeType.MERGE, mappedBy = "metadataSegment", fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("segment_properties")
    private List<MetadataSegmentProperty> metadataSegmentProperties = new ArrayList<>();

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

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("restriction")
    public Restriction getRestriction() {
        return JsonUtils.deserialize(restrictionString, Restriction.class);
    }

    @JsonProperty("restriction")
    public void setRestriction(Restriction restriction) {
        this.restrictionString = JsonUtils.serialize(restriction);
    }

    public FlattenedRestriction getSimpleRestriction() {
        return simpleRestriction;
    }

    public void setSimpleRestriction(FlattenedRestriction simpleRestriction) {
        this.simpleRestriction = simpleRestriction;
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

    public List<MetadataSegmentProperty> getMetadataSegmentProperties() {
        return metadataSegmentProperties;
    }

    public void setMetadataSegmentProperties(List<MetadataSegmentProperty> metadataSegmentProperties) {
        this.metadataSegmentProperties = metadataSegmentProperties;
    }

    public void addSegmentProperty(MetadataSegmentProperty metadataSegmentProperty) {
        this.metadataSegmentProperties.add(metadataSegmentProperty);
    }

    @Transient
    @JsonIgnore
    public MetadataSegmentPropertyBag getSegmentPropertyBag() {
        return new MetadataSegmentPropertyBag(metadataSegmentProperties);
    }

    @Transient
    @JsonIgnore
    public void setSegmentPropertyBag(MetadataSegmentPropertyBag metadataSegmentPropertyBag) {
        this.metadataSegmentProperties = metadataSegmentPropertyBag.getBag();
    }
}
