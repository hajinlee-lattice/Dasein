package com.latticeengines.domain.exposed.pls;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.query.CaseLookup;

@Entity
@javax.persistence.Table(name = "RULE_BASED_MODEL")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RuleBasedModel extends RatingModel {

    public static final String RULE_BASED_MODEL_PREFIX = "rule_based_model";
    public static final String RULE_BASED_MODEL_FORMAT = "%s__%s";

    public RuleBasedModel() {
    }

    @JsonIgnore
    @Column(name = "CASE_LOOKUP", nullable = true)
    @Type(type = "text")
    private String caseLookupStr;

    @JsonProperty("caseLookup")
    public void setCaseLookup(CaseLookup caseLookup) {
        this.caseLookupStr = caseLookup.toString();
    }

    @JsonProperty("caseLookup")
    public CaseLookup getCaseLookup() {
        return JsonUtils.deserialize(this.caseLookupStr, CaseLookup.class);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public String generateIdStr() {
        return String.format(RULE_BASED_MODEL_FORMAT, RULE_BASED_MODEL_PREFIX,
                UuidUtils.shortenUuid(UUID.randomUUID()));
    }

}
