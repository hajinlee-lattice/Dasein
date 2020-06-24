package com.latticeengines.domain.exposed.dcp.match;

import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "DCP_MATCH_RULE",
        indexes = {
                @Index(name = "IX_MATCH_RULE_ID", columnList = "MATCH_RULE_ID"),
                @Index(name = "IX_SOURCE_ID", columnList = "SOURCE_ID")},
        uniqueConstraints = {
                @UniqueConstraint(name = "UX_MATCH_VERSION", columnNames = {"FK_TENANT_ID", "MATCH_RULE_ID", "VERSION_ID"})})
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class MatchRuleRecord implements HasPid, HasTenant, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @Column(name = "MATCH_RULE_ID")
    private String matchRuleId;

    @Column(name = "SOURCE_ID")
    private String sourceId;

    @Column(name = "DISPLAY_NAME")
    private String displayName;

    @Column(name = "STATE", length = 30)
    @Enumerated(EnumType.STRING)
    private State state;

    @Column(name = "RULE_TYPE", length = 30)
    @Enumerated(EnumType.STRING)
    private RuleType ruleType;

    @Column(name = "MATCH_KEY", length = 30)
    @Enumerated(EnumType.STRING)
    private MatchKey matchKey;

    @Column(name = "VERSION_ID")
    private Integer versionId;

    @Column(name = "ALLOWED_VALUES", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<String> allowedValues;

    @Column(name = "EXCLUSION_CRITERION_LIST", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<ExclusionCriterion> exclusionCriterionList;

    @Column(name = "ACCEPT_CRITERION", columnDefinition = "'JSON'")
    @Type(type = "json")
    private DplusMatchRule.ClassificationCriterion acceptCriterion;

    @Column(name = "REVIEW_CRITERION", columnDefinition = "'JSON'")
    @Type(type = "json")
    private DplusMatchRule.ClassificationCriterion reviewCriterion;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date date) {
        this.created = date;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date date) {
        this.updated = date;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

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

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public RuleType getRuleType() {
        return ruleType;
    }

    public void setRuleType(RuleType ruleType) {
        this.ruleType = ruleType;
    }

    public MatchKey getMatchKey() {
        return matchKey;
    }

    public void setMatchKey(MatchKey matchKey) {
        this.matchKey = matchKey;
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

    public enum State {
        ACTIVE, INACTIVE, ARCHIVED
    }

    public enum RuleType {
        BASE_RULE, SPECIAL_RULE
    }
}
