package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.query.AttributeLookup;

@Entity(name = "RATING_MODEL")
@Inheritance(strategy = InheritanceType.JOINED)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = RuleBasedModel.class, name = "rule"), //
        @Type(value = AIModel.class, name = "AI") })
@NamedEntityGraph(name = "RatingModel.ratingEngine", attributeNodes = @NamedAttributeNode("ratingEngine"))
public abstract class RatingModel implements HasPid, HasId<String>, HasAuditingFields {

    protected RatingEngine ratingEngine;
    private Long pid;
    private String id;
    private int iteration = 1;
    private Date created;
    private Date updated;
    private String createdBy;
    private String updatedBy;
    private String derivedFromRatingModel;

    private Set<AttributeLookup> ratingModelAttributes;

    @Override
    @JsonProperty("pid")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", nullable = false)
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonProperty("id")
    @Column(name = "ID", nullable = false, unique = true)
    public String getId() {
        return this.id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @CreatedDate
    public Date getCreated() {
        return this.created;
    }

    @Override
    public void setCreated(Date time) {
        this.created = time;
    }

    @Override
    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    @LastModifiedDate
    public Date getUpdated() {
        return this.updated;
    }

    @Override
    public void setUpdated(Date time) {
        this.updated = time;
    }

    @JsonProperty("createdBy")
    @Column(name = "CREATED_BY")
    public String getCreatedBy() {
        return this.createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @JsonProperty("updatedBy")
    @Column(name = "UPDATED_BY")
    public String getUpdatedBy() {
        return this.updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    @JsonProperty("iteration")
    @Column(name = "ITERATION", nullable = false)
    public int getIteration() {
        return this.iteration;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_RATING_ENGINE_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public RatingEngine getRatingEngine() {
        return this.ratingEngine;
    }

    public void setRatingEngine(RatingEngine ratingEngine) {
        this.ratingEngine = ratingEngine;
    }

    @JsonProperty("ratingmodel_attributes")
    @Transient
    public Set<AttributeLookup> getRatingModelAttributes() {
        return this.ratingModelAttributes;
    }

    public void setRatingModelAttributes(Set<AttributeLookup> attributes) {
        this.ratingModelAttributes = attributes;
    }

    @JsonProperty("derived_from_rating_model")
    @Column(name = "DERIVED_FROM_RATING_MODEL")
    public String getDerivedFromRatingModel() {
        return derivedFromRatingModel;
    }

    public void setDerivedFromRatingModel(String derivedFromRatingModel) {
        this.derivedFromRatingModel = derivedFromRatingModel;
    }
}
