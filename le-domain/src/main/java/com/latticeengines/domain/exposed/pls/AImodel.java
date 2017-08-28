package com.latticeengines.domain.exposed.pls;

import java.util.UUID;

import javax.persistence.Entity;
import javax.persistence.Transient;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.UuidUtils;

@Entity
@javax.persistence.Table(name = "AI_MODEL")
@JsonIgnoreProperties(ignoreUnknown = true)
@OnDelete(action = OnDeleteAction.CASCADE)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class AImodel extends RatingModel {

    public static final String AI_MODEL_PREFIX = "ai_model";
    public static final String AI_MODEL_FORMAT = "%s__%s";

    // TODO in M14, we do not support AImodel
    // @OneToMany(cascade = { CascadeType.ALL }, mappedBy = "aImodel")
    @Transient
    @JsonProperty("model")
    private ModelSummary model;

    public void setModel(ModelSummary model) {
        this.model = model;
    }

    public ModelSummary getModel() {
        return this.model;
    }

    public static String generateIdStr() {
        return String.format(AI_MODEL_FORMAT, AI_MODEL_PREFIX, UuidUtils.shortenUuid(UUID.randomUUID()));
    }
}
