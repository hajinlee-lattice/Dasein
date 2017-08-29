package com.latticeengines.domain.exposed.pls;

import java.util.Date;

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
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

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

@Entity(name = "RATING_MODEL")
@Inheritance(strategy = InheritanceType.JOINED)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = RuleBasedModel.class, name = "rule"), //
        @Type(value = AImodel.class, name = "AI") })
public abstract class RatingModel implements HasPid, HasId<String>, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.TABLE)
    @JsonIgnore
    @Column(name = "PID", nullable = false)
    private Long pid;

    @JsonProperty("id")
    @Column(name = "ID", nullable = false, unique = true)
    private String id;

    @JsonProperty("iteration")
    @Column(name = "Iteration", nullable = false)
    private int iteration = 1;

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_RATING_ENGINE_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    protected RatingEngine ratingEngine;

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void setCreated(Date time) {
        this.created = time;
    }

    @Override
    public Date getCreated() {
        return this.created;
    }

    @Override
    public void setUpdated(Date time) {
        this.updated = time;
    }

    @Override
    public Date getUpdated() {
        return this.updated;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public int getIteration() {
        return this.iteration;
    }

    public void setRatingEngine(RatingEngine ratingEngine) {
        this.ratingEngine = ratingEngine;
    }

    public RatingEngine getRatingEngine() {
        return this.ratingEngine;
    }
}
