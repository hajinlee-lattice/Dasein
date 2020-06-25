package com.latticeengines.apps.dcp.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.dcp.entitymgr.MatchRuleEntityMgr;
import com.latticeengines.apps.dcp.service.MatchRuleService;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleConfiguration;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;
import com.latticeengines.redis.lock.RedisDistributedLock;

@Service("matchRuleService")
public class MatchRuleServiceImpl implements MatchRuleService {

    private static final Logger log = LoggerFactory.getLogger(MatchRuleServiceImpl.class);

    private static final String RANDOM_MATCH_RULE_ID_PATTERN = "MatchRule_%s";

    @Inject
    private MatchRuleEntityMgr matchRuleEntityMgr;

    @Inject
    private RedisDistributedLock redisDistributedLock;

    @Override
    public MatchRule updateMatchRule(String customerSpace, MatchRule matchRule) {
        if (matchRule == null) {
            throw new IllegalArgumentException("Cannot update NULL match rule!");
        }
        MatchRuleRecord matchRuleRecord = matchRuleEntityMgr.findTopActiveMatchRule(matchRule.getMatchRuleId());
        if (matchRuleRecord == null) {
            throw new IllegalArgumentException("Cannot find active match rule to update with id: " + matchRule.getMatchRuleId());
        }
        Pair<Boolean, Boolean> pair = onlyDisplayNameChange(matchRuleRecord, matchRule);
        if (pair.getLeft()) {
            matchRuleEntityMgr.updateMatchRule(matchRule.getMatchRuleId(), matchRule.getDisplayName());
        }
        if (pair.getRight()) {
            // update top record to INACTIVE
            matchRuleRecord.setState(MatchRuleRecord.State.INACTIVE);
            matchRuleEntityMgr.update(matchRuleRecord);

            // create new Active record
            MatchRuleRecord newRecord = new MatchRuleRecord();
            newRecord.setMatchRuleId(matchRule.getMatchRuleId());
            newRecord.setSourceId(matchRule.getSourceId());
            newRecord.setDisplayName(matchRule.getDisplayName());
            newRecord.setRuleType(matchRule.getRuleType());
            newRecord.setMatchKey(matchRule.getMatchKey());
            newRecord.setAllowedValues(matchRule.getAllowedValues());
            newRecord.setExclusionCriterionList(matchRule.getExclusionCriterionList());
            newRecord.setAcceptCriterion(matchRule.getAcceptCriterion());
            newRecord.setReviewCriterion(matchRule.getReviewCriterion());

            newRecord.setVersionId(matchRuleRecord.getVersionId() + 1);
            newRecord.setState(MatchRuleRecord.State.ACTIVE);

            matchRuleEntityMgr.create(newRecord);
            return convertMatchRuleRecord(newRecord);

        }
        return convertMatchRuleRecord(matchRuleRecord);
    }

    // Left : displayName change, right: other fields change.
    private Pair<Boolean, Boolean> onlyDisplayNameChange(MatchRuleRecord record, MatchRule matchRule) {
        boolean displayNameChange = !record.getDisplayName().equals(matchRule.getDisplayName());
        boolean otherFieldUnChange = record.getRuleType() == matchRule.getRuleType();
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        otherFieldUnChange = record.getMatchKey() ==  matchRule.getMatchKey();
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        otherFieldUnChange = CollectionUtils.size(record.getAllowedValues()) == CollectionUtils.size(matchRule.getAllowedValues());
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        if (record.getAllowedValues() != null) {
            otherFieldUnChange = CollectionUtils.isEqualCollection(record.getAllowedValues(), matchRule.getAllowedValues());
        }
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        otherFieldUnChange = CollectionUtils.size(record.getExclusionCriterionList()) == CollectionUtils.size(matchRule.getExclusionCriterionList());
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        if (record.getExclusionCriterionList() != null) {
            otherFieldUnChange = CollectionUtils.isEqualCollection(record.getExclusionCriterionList(), matchRule.getExclusionCriterionList());
        }
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        otherFieldUnChange = (record.getAcceptCriterion() == null && matchRule.getAcceptCriterion() == null)
                        || (record.getAcceptCriterion() != null && matchRule.getAcceptCriterion() != null);
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        if (record.getAcceptCriterion() != null) {
            otherFieldUnChange = record.getAcceptCriterion().equalTo(matchRule.getAcceptCriterion());
        }
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        otherFieldUnChange = (record.getReviewCriterion() == null && matchRule.getReviewCriterion() == null)
                || (record.getReviewCriterion() != null && matchRule.getReviewCriterion() != null);
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        if (record.getReviewCriterion() != null) {
            otherFieldUnChange = record.getReviewCriterion().equalTo(matchRule.getReviewCriterion());
        }
        if (!otherFieldUnChange) {
            return Pair.of(displayNameChange, true);
        }
        return Pair.of(displayNameChange, false);
    }

    @Override
    public MatchRule createMatchRule(String customerSpace, MatchRule matchRule) {
        Preconditions.checkNotNull(matchRule);
        if (MatchRuleRecord.RuleType.BASE_RULE.equals(matchRule.getRuleType())) {
            if (matchRuleEntityMgr.existMatchRule(matchRule.getSourceId(), matchRule.getRuleType())) {
                throw new IllegalArgumentException("Already has an active Base Match Rule, cannot create a new one!");
            }
        }
        MatchRuleRecord matchRuleRecord = new MatchRuleRecord();
        matchRuleRecord.setSourceId(matchRule.getSourceId());
        matchRuleRecord.setDisplayName(matchRule.getDisplayName());
        matchRuleRecord.setRuleType(matchRule.getRuleType());
        matchRuleRecord.setMatchKey(matchRule.getMatchKey());
        matchRuleRecord.setAllowedValues(matchRule.getAllowedValues());
        matchRuleRecord.setExclusionCriterionList(matchRule.getExclusionCriterionList());
        matchRuleRecord.setAcceptCriterion(matchRule.getAcceptCriterion());
        matchRuleRecord.setReviewCriterion(matchRule.getReviewCriterion());

        matchRuleRecord.setMatchRuleId(generateRandomMatchRuleId());
        matchRuleRecord.setVersionId(1);
        matchRuleRecord.setState(MatchRuleRecord.State.ACTIVE);

        matchRuleEntityMgr.create(matchRuleRecord);
        return convertMatchRuleRecord(matchRuleRecord);
    }

    @Override
    public List<MatchRule> getMatchRuleList(String customerSpace, String sourceId, Boolean includeArchived, Boolean includeInactive) {
        List<MatchRuleRecord> activeMatchRules = matchRuleEntityMgr.findMatchRules(sourceId,
                MatchRuleRecord.State.ACTIVE);
        List<MatchRuleRecord> inactiveMatchRules = null;
        if (Boolean.TRUE.equals(includeArchived)) {
            inactiveMatchRules = matchRuleEntityMgr.findMatchRules(sourceId, MatchRuleRecord.State.INACTIVE);
        }
        List<MatchRuleRecord> archivedMatchRules = null;
        if (Boolean.TRUE.equals(includeArchived)) {
            archivedMatchRules = matchRuleEntityMgr.findMatchRules(sourceId, MatchRuleRecord.State.ARCHIVED);
        }
        List<MatchRule> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(activeMatchRules)) {
            activeMatchRules.forEach(record -> result.add(convertMatchRuleRecord(record)));
        }
        if (CollectionUtils.isNotEmpty(inactiveMatchRules)) {
            inactiveMatchRules.forEach(record -> result.add(convertMatchRuleRecord(record)));
        }
        if (CollectionUtils.isNotEmpty(archivedMatchRules)) {
            archivedMatchRules.forEach(record -> result.add(convertMatchRuleRecord(record)));
        }
        return result;
    }

    @Override
    public void archiveMatchRule(String customerSpace, String matchRuleId) {
        MatchRuleRecord matchRuleRecord = matchRuleEntityMgr.findTopActiveMatchRule(matchRuleId);
        if (matchRuleRecord == null) {
            throw new IllegalArgumentException("Cannot find active match rule to archive with id: " + matchRuleId);
        }
        if (MatchRuleRecord.RuleType.BASE_RULE.equals(matchRuleRecord.getRuleType())) {
            throw new IllegalArgumentException("Cannot archive Base Match Rule!");
        }
        matchRuleEntityMgr.updateMatchRule(matchRuleId, MatchRuleRecord.State.ARCHIVED);
    }

    @Override
    public MatchRuleConfiguration getMatchConfig(String customerSpace, String sourceId) {
        List<MatchRule> matchRules = getMatchRuleList(customerSpace, sourceId, Boolean.FALSE, Boolean.FALSE);
        if (CollectionUtils.isEmpty(matchRules)) {
            return null;
        }
        Optional<MatchRule> baseRule =
                matchRules.stream().filter(rule -> MatchRuleRecord.RuleType.BASE_RULE.equals(rule.getRuleType())).findAny();
        if (baseRule.isPresent()) {
            MatchRuleConfiguration matchRuleConfiguration = new MatchRuleConfiguration();
            matchRuleConfiguration.setBaseRule(baseRule.get());
            matchRuleConfiguration.setSpecialRules(matchRules.stream()
                    .filter(rule -> MatchRuleRecord.RuleType.SPECIAL_RULE.equals(rule.getRuleType()))
                    .collect(Collectors.toList()));
            return matchRuleConfiguration;
        } else {
            log.warn("Cannot build MatchConfiguration for source: " + sourceId);
            return null;
        }
    }

    private MatchRule convertMatchRuleRecord(MatchRuleRecord record) {
        MatchRule matchRule = new MatchRule();
        matchRule.setSourceId(record.getSourceId());
        matchRule.setDisplayName(record.getDisplayName());
        matchRule.setRuleType(record.getRuleType());
        matchRule.setMatchKey(record.getMatchKey());
        matchRule.setAllowedValues(record.getAllowedValues());
        matchRule.setExclusionCriterionList(record.getExclusionCriterionList());
        matchRule.setAcceptCriterion(record.getAcceptCriterion());
        matchRule.setReviewCriterion(record.getReviewCriterion());
        matchRule.setMatchRuleId(record.getMatchRuleId());
        matchRule.setVersionId(record.getVersionId());
        matchRule.setState(record.getState());
        matchRule.setCreated(record.getCreated());
        return matchRule;
    }

    private String generateRandomMatchRuleId() {
        String randomMatchRuleId = String.format(RANDOM_MATCH_RULE_ID_PATTERN,
                RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        while (matchRuleEntityMgr.existMatchRule(randomMatchRuleId)) {
            randomMatchRuleId = String.format(RANDOM_MATCH_RULE_ID_PATTERN,
                    RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        return randomMatchRuleId;
    }
}
