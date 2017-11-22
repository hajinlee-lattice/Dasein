package com.latticeengines.domain.exposed.pls;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
@javax.persistence.Table(name = "RATING_ENGINE_NOTE")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RatingEngineNote extends Note {

    private static final long serialVersionUID = 1L;

    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_RATING_ENGINE_ID", nullable = false)
    @JsonIgnore
    private RatingEngine ratingEngine;

    public void setRatingEngine(RatingEngine ratingEngine) {
        this.ratingEngine = ratingEngine;
    }

    public RatingEngine getRatingEngine() {
        return this.ratingEngine;
    }

}
