package com.latticeengines.domain.exposed.dcp.match;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MatchRule {

    @JsonProperty("matchRuleId")
    private String matchRuleId;

    @JsonProperty("sourceId")
    private String sourceId;

    @JsonProperty("displayName")
    private String displayName;

    @JsonProperty("state")
    private MatchRuleRecord.State state;

    @JsonProperty("created")
    private Date created;

    @JsonProperty("ruleType")
    private MatchRuleRecord.RuleType ruleType;

    @JsonProperty("matchKey")
    private MatchKey matchKey;

    @JsonProperty("domain")
    private DataDomain domain;

    @JsonProperty("recordType")
    private DataRecordType recordType;

    @JsonProperty("versionId")
    private Integer versionId;

    @JsonProperty("allowedValues")
    private List<String> allowedValues;

    @JsonProperty("exclusionCriterion")
    private List<ExclusionCriterion> exclusionCriterionList;

    @JsonProperty("acceptCriterion")
    private DplusMatchRule.ClassificationCriterion acceptCriterion;

    @JsonProperty("reviewCriterion")
    private DplusMatchRule.ClassificationCriterion reviewCriterion;

    public String getMatchRuleId() {
        return matchRuleId;
    }

    public void setMatchRuleId(String matchRuleId) {
        this.matchRuleId = matchRuleId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public MatchRuleRecord.State getState() {
        return state;
    }

    public void setState(MatchRuleRecord.State state) {
        this.state = state;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public MatchRuleRecord.RuleType getRuleType() {
        return ruleType;
    }

    public void setRuleType(MatchRuleRecord.RuleType ruleType) {
        this.ruleType = ruleType;
    }

    public MatchKey getMatchKey() {
        return matchKey;
    }

    public void setMatchKey(MatchKey matchKey) {
        this.matchKey = matchKey;
    }

    public DataDomain getDomain() {
        return domain;
    }

    public void setDomain(DataDomain domain) {
        this.domain = domain;
    }

    public DataRecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(DataRecordType recordType) {
        this.recordType = recordType;
    }

    public Integer getVersionId() {
        return versionId;
    }

    public void setVersionId(Integer versionId) {
        this.versionId = versionId;
    }

    public List<String> getAllowedValues() {
        return allowedValues;
    }

    public void setAllowedValues(List<String> allowedValues) {
        this.allowedValues = allowedValues;
    }

    public List<ExclusionCriterion> getExclusionCriterionList() {
        return exclusionCriterionList;
    }

    public void setExclusionCriterionList(List<ExclusionCriterion> exclusionCriterionList) {
        this.exclusionCriterionList = exclusionCriterionList;
    }

    public DplusMatchRule.ClassificationCriterion getAcceptCriterion() {
        return acceptCriterion;
    }

    public void setAcceptCriterion(DplusMatchRule.ClassificationCriterion acceptCriterion) {
        this.acceptCriterion = acceptCriterion;
    }

    public DplusMatchRule.ClassificationCriterion getReviewCriterion() {
        return reviewCriterion;
    }

    public void setReviewCriterion(DplusMatchRule.ClassificationCriterion reviewCriterion) {
        this.reviewCriterion = reviewCriterion;
    }
}
