package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.apache.commons.lang.StringUtils;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;

@Entity
@javax.persistence.Table(name = "RULE_BASED_MODEL")
@JsonIgnoreProperties(ignoreUnknown = true)
@OnDelete(action = OnDeleteAction.CASCADE)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RuleBasedModel extends RatingModel {

    public static final String RULE_BASED_MODEL_PREFIX = "rule_based_model";
    public static final String RULE_BASED_MODEL_FORMAT = "%s__%s";

    public RuleBasedModel() {
    }

    @JsonIgnore
    @Column(name = "RULE", nullable = true)
    @Type(type = "text")
    private String ratingRule;

    @JsonIgnore
    @Column(name = "SELECTED_ATTRIBUTES", nullable = true)
    @Type(type = "text")
    private String selectedAttributes;

    @JsonProperty("ratingRule")
    public void setRatingRule(RatingRule ratingRule) {
        this.ratingRule = JsonUtils.serialize(ratingRule);
    }

    @JsonProperty("ratingRule")
    public RatingRule getRatingRule() {
        return JsonUtils.deserialize(this.ratingRule, RatingRule.class);
    }

    @JsonProperty("selectedAttributes")
    public void setSelectedAttributes(List<String> selectedAttributes) {
        this.selectedAttributes = JsonUtils.serialize(selectedAttributes);
    }

    @JsonProperty("selectedAttributes")
    public List<String> getSelectedAttributes() {
        List<String> attrList = null;
        if (StringUtils.isNotBlank(this.selectedAttributes)) {
            List<?> attrListIntermediate = JsonUtils.deserialize(this.selectedAttributes, List.class);
            attrList = JsonUtils.convertList(attrListIntermediate, String.class);
        }

        return attrList;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public static String generateIdStr() {
        return String.format(RULE_BASED_MODEL_FORMAT, RULE_BASED_MODEL_PREFIX,
                UuidUtils.shortenUuid(UUID.randomUUID()));
    }

}
