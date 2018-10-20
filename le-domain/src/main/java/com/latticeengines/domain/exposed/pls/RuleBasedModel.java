package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;

import io.swagger.annotations.ApiModel;

@Entity
@javax.persistence.Table(name = "RULE_BASED_MODEL")
@JsonIgnoreProperties(ignoreUnknown = true)
@OnDelete(action = OnDeleteAction.CASCADE)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@ApiModel("Represents RuleBasedModel JSON Object")
public class RuleBasedModel extends RatingModel {

    public static final String RULE_BASED_MODEL_PREFIX = "rule";
    public static final String RULE_BASED_MODEL_FORMAT = "%s_%s";
    private RatingRule ratingRule;
    private List<String> selectedAttributes;

    public RuleBasedModel() {
    }

    public static String generateIdStr() {
        String uuid = AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID()));
        return String.format(RULE_BASED_MODEL_FORMAT, RULE_BASED_MODEL_PREFIX, uuid);
    }

    @JsonIgnore
    @Column(name = "RULE", nullable = true)
    @Type(type = "text")
    public String getRatingRuleString() {
        String ratingRuleStr = null;
        if (this.ratingRule != null) {
            ratingRuleStr = JsonUtils.serialize(this.ratingRule);
        }
        return ratingRuleStr;
    }

    public void setRatingRuleString(String ratingRuleStr) {
        if (StringUtils.isNotBlank(ratingRuleStr)) {
            this.ratingRule = JsonUtils.deserialize(ratingRuleStr, RatingRule.class);
        }
    }

    @Transient
    @JsonProperty("ratingRule")
    public RatingRule getRatingRule() {
        return this.ratingRule;
    }

    public void setRatingRule(RatingRule ratingRule) {
        this.ratingRule = ratingRule;
    }

    @JsonIgnore
    @Column(name = "SELECTED_ATTRIBUTES", nullable = true)
    @Type(type = "text")
    public String getSelectedAttributesString() {
        String selectedAttributesStr = null;
        if (this.selectedAttributes != null) {
            selectedAttributesStr = JsonUtils.serialize(this.selectedAttributes);
        }
        return selectedAttributesStr;
    }

    public void setSelectedAttributesString(String selectedAttributesStr) {
        if (StringUtils.isNotBlank(selectedAttributesStr)) {
            List<?> tempSelectedAttributes = JsonUtils.deserialize(selectedAttributesStr,
                    List.class);
            this.selectedAttributes = JsonUtils.convertList(tempSelectedAttributes, String.class);
        }
    }

    @Transient
    @JsonProperty("selectedAttributes")
    public List<String> getSelectedAttributes() {
        return this.selectedAttributes;
    }

    public void setSelectedAttributes(List<String> selectedAttributes) {
        this.selectedAttributes = selectedAttributes;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
