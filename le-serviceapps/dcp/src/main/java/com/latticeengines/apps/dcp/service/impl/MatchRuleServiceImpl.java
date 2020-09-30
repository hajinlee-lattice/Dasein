package com.latticeengines.apps.dcp.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.dcp.entitymgr.MatchRuleEntityMgr;
import com.latticeengines.apps.dcp.service.AppendConfigService;
import com.latticeengines.apps.dcp.service.MatchRuleService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleConfiguration;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.redis.lock.RedisDistributedLock;

import avro.shaded.com.google.common.collect.Sets;

@Service("matchRuleService")
public class MatchRuleServiceImpl implements MatchRuleService {

    private static final Logger log = LoggerFactory.getLogger(MatchRuleServiceImpl.class);

    private static final String RANDOM_MATCH_RULE_ID_PATTERN = "MatchRule_%s";

    @Inject
    private MatchRuleEntityMgr matchRuleEntityMgr;

    @Inject
    private RedisDistributedLock redisDistributedLock;

    @Inject
    private AppendConfigService appendConfigService;

    @Value("${dcp.lock.namespace}")
    private String lockNameSpace;

    @Override
    public MatchRule updateMatchRule(String customerSpace, MatchRule matchRule) {
        Preconditions.checkNotNull(matchRule);
        if (StringUtils.isEmpty(matchRule.getMatchRuleId())) {
            throw new IllegalArgumentException("Cannot update match rule without matchRuleId!");
        }
        String lockKey = getLockKey("", matchRule.getMatchRuleId(), false);
        String requestId = UUID.randomUUID().toString();
        if (redisDistributedLock.lock(lockKey, requestId, 30000, true)) {
            try {
                MatchRuleRecord matchRuleRecord = matchRuleEntityMgr.findTopActiveMatchRule(matchRule.getMatchRuleId());
                if (matchRuleRecord == null) {
                    throw new IllegalArgumentException("Cannot find active match rule to update with id: " + matchRule.getMatchRuleId());
                }
                if (!matchRuleRecord.getSourceId().equals(matchRule.getSourceId())) {
                    throw new IllegalArgumentException(String.format("Cannot update Match Rule from source %s to source %s",
                            matchRuleRecord.getSourceId(), matchRule.getSourceId()));
                }
                validateMatchRuleForUpdate(customerSpace, matchRule, matchRuleRecord);
                if (MatchRuleRecord.RuleType.SPECIAL_RULE.equals(matchRule.getRuleType())) {
                    List<MatchRuleRecord> currentRecords = matchRuleEntityMgr.findMatchRules(matchRule.getSourceId(),
                            MatchRuleRecord.State.ACTIVE);
                    if (CollectionUtils.isNotEmpty(currentRecords)) {
                        currentRecords.removeIf(record -> record.getMatchRuleId().equals(matchRule.getMatchRuleId()));
                    }
                    checkIntersection(matchRule, currentRecords);
                }
                Pair<Boolean, Boolean> pair = onlyDisplayNameChange(matchRuleRecord, matchRule);
                if (pair.getLeft()) {
                    matchRuleEntityMgr.updateMatchRule(matchRule.getMatchRuleId(), matchRule.getDisplayName());
                    matchRuleRecord.setDisplayName(matchRule.getDisplayName());
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
                    newRecord.setDomain(matchRule.getDomain());
                    newRecord.setRecordType(matchRule.getRecordType());
                    newRecord.setAllowedValues(matchRule.getAllowedValues());
                    newRecord.setExclusionCriterionList(matchRule.getExclusionCriterionList());
                    newRecord.setAcceptCriterion(matchRule.getAcceptCriterion());
                    newRecord.setReviewCriterion(matchRule.getReviewCriterion());

                    newRecord.setVersionId(matchRuleRecord.getVersionId() + 1);
                    newRecord.setState(MatchRuleRecord.State.ACTIVE);

                    newRecord.setTenant(MultiTenantContext.getTenant());

                    matchRuleEntityMgr.create(newRecord);
                    return convertMatchRuleRecord(newRecord);

                }
                return convertMatchRuleRecord(matchRuleRecord);
            } finally {
                redisDistributedLock.releaseLock(lockKey, requestId);
            }
        }
        throw new RuntimeException("Cannot get update lock, please try again later.");
    }

    /**
     * Check if the AllowedValues in new Rule intersect with current active rules.
     * @param newRule The Match rule to be updated or created, needs to be a SPECIAL_RULE
     * @param activeRuleRecords Current Active MatchRules but not include the newRule matchId (if there's one)
     */
    private void checkIntersection(MatchRule newRule, List<MatchRuleRecord> activeRuleRecords) {
        if (CollectionUtils.isEmpty(activeRuleRecords)
                || newRule.getMatchKey() == null
                || CollectionUtils.isEmpty(newRule.getAllowedValues())) {
            return;
        }
        List<String> allowedValues = activeRuleRecords.stream()
                .filter(record -> MatchRuleRecord.RuleType.SPECIAL_RULE.equals(record.getRuleType())
                        && newRule.getMatchKey().equals(record.getMatchKey()))
                .map(MatchRuleRecord::getAllowedValues)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(allowedValues)) {
            return;
        }
        Set<String> interSection = newRule.getAllowedValues().stream().distinct().filter(allowedValues::contains).collect(Collectors.toSet());

        if (CollectionUtils.isNotEmpty(interSection)) {
            throw new IllegalArgumentException("Already contains following allowed values: " + Strings.join(interSection, ","));
        }
    }

    /**
     * Validate Rule for update
     * 1. MatchKey cannot be null for Special rule and must be null for Base rule.
     * 2. AllowedValues cannot be empty for Special rule and must be empty for Base rule.
     * 3. RuleType cannot be changed.
     * 4. If MatchKey is Country, AllowedValues should be ISO-3166 alpha-2 code
     * 5. Verify if Domain & RecordType entitled with company entity resolution.
     * @param newRule The new match rule
     * @param oldRuleRecord The original match rule
     */
    private void validateMatchRuleForUpdate(String customerSpace, MatchRule newRule, MatchRuleRecord oldRuleRecord) {
        if (MatchRuleRecord.RuleType.SPECIAL_RULE.equals(newRule.getRuleType())) {
            if (newRule.getMatchKey() == null) {
                throw new IllegalArgumentException("Match Key cannot be NULL for a special match rule!");
            }
            if (CollectionUtils.isEmpty(newRule.getAllowedValues())) {
                throw new IllegalArgumentException("AllowedValues cannot be empty for a special match rule!");
            }
        } else if (MatchRuleRecord.RuleType.BASE_RULE.equals(newRule.getRuleType())) {
            if (newRule.getMatchKey() != null) {
                throw new IllegalArgumentException("Base match rule cannot have MatchKey!");
            }
            if (CollectionUtils.isNotEmpty(newRule.getAllowedValues())) {
                throw new IllegalArgumentException("Base match rule cannot have allowed values!");
            }
        }
        if (!newRule.getRuleType().equals(oldRuleRecord.getRuleType())) {
            throw new IllegalArgumentException(String.format("Cannot update match rule type from %s to %s",
                    oldRuleRecord.getRuleType(), newRule.getRuleType()));
        }
        if (MatchKey.Country.equals(newRule.getMatchKey())) {
            validateCountryCode(newRule.getAllowedValues());
        }
        if (!appendConfigService.checkEntitledWith(customerSpace, newRule.getDomain(),
                newRule.getRecordType(), DataBlock.BLOCK_COMPANY_ENTITY_RESOLUTION)) {
            throw new LedpException(LedpCode.LEDP_60011);
        }
    }

    /**
     * Validate Rule for create
     * 1. MatchKey cannot be null for Special rule and must be null for Base rule.
     * 2. AllowedValues cannot be empty for Special rule and must be empty for Base rule.
     * 3. If MatchKey is Country, AllowedValues should be ISO-3166 alpha-2 code
     * 4. Verify if Domain & RecordType entitled with company entity resolution.
     * @param matchRule Match Rule to be created
     */
    private void validateMatchRuleForCreate(String customerSpace, MatchRule matchRule) {
        if (MatchRuleRecord.RuleType.SPECIAL_RULE.equals(matchRule.getRuleType())) {
            if (matchRule.getMatchKey() == null) {
                throw new IllegalArgumentException("Match Key cannot be NULL for a special match rule!");
            }
            if (CollectionUtils.isEmpty(matchRule.getAllowedValues())) {
                throw new IllegalArgumentException("AllowedValues cannot be empty for a special match rule!");
            }
        } else if (MatchRuleRecord.RuleType.BASE_RULE.equals(matchRule.getRuleType())) {
            if (matchRule.getMatchKey() != null) {
                throw new IllegalArgumentException("Base match rule cannot have MatchKey!");
            }
            if (CollectionUtils.isNotEmpty(matchRule.getAllowedValues())) {
                throw new IllegalArgumentException("Base match rule cannot have allowed values!");
            }
        }
        if (MatchKey.Country.equals(matchRule.getMatchKey())) {
            validateCountryCode(matchRule.getAllowedValues());
        }
        if (!appendConfigService.checkEntitledWith(customerSpace, matchRule.getDomain(),
                matchRule.getRecordType(), DataBlock.BLOCK_COMPANY_ENTITY_RESOLUTION)) {
            throw new LedpException(LedpCode.LEDP_60011);
        }
    }

    private void validateCountryCode(List<String> values) {
        String[] isoAlpha2Countries = Locale.getISOCountries();
        Set<String> countryCodeSet = Sets.newHashSet(isoAlpha2Countries);
        values.forEach(code -> {
            if (code.length() != 2 || !countryCodeSet.contains(code.toUpperCase())) {
                throw new IllegalArgumentException("Unrecognized ISO-3166 alpha-2 code: " + code);
            }
        });
    }

    private String getLockKey(String sourceId, String matchRuleId, boolean create) {
        if (create) {
            return lockNameSpace + "." + sourceId;
        } else {
            return lockNameSpace + "." + matchRuleId;
        }
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
        if (StringUtils.isEmpty(matchRule.getSourceId())) {
            throw new IllegalArgumentException("Cannot create Match Rule without source!");
        }
        validateMatchRuleForCreate(customerSpace, matchRule);
        String lockKey = getLockKey(matchRule.getSourceId(), "", true);
        String requestId = UUID.randomUUID().toString();
        if (redisDistributedLock.lock(lockKey, requestId, 30000, true)) {
            try {
                if (MatchRuleRecord.RuleType.BASE_RULE.equals(matchRule.getRuleType())) {
                    if (matchRuleEntityMgr.existMatchRule(matchRule.getSourceId(), matchRule.getRuleType())) {
                        throw new IllegalArgumentException("Already has an active Base Match Rule, cannot create a new one!");
                    }
                } else if (MatchRuleRecord.RuleType.SPECIAL_RULE.equals(matchRule.getRuleType())) {
                    List<MatchRuleRecord> currentRecords = matchRuleEntityMgr.findMatchRules(matchRule.getSourceId(),
                            MatchRuleRecord.State.ACTIVE);
                    checkIntersection(matchRule, currentRecords);
                }
                MatchRuleRecord matchRuleRecord = new MatchRuleRecord();
                matchRuleRecord.setSourceId(matchRule.getSourceId());
                matchRuleRecord.setDisplayName(matchRule.getDisplayName());
                matchRuleRecord.setRuleType(matchRule.getRuleType());
                matchRuleRecord.setMatchKey(matchRule.getMatchKey());
                matchRuleRecord.setDomain(matchRule.getDomain());
                matchRuleRecord.setRecordType(matchRule.getRecordType());
                matchRuleRecord.setAllowedValues(matchRule.getAllowedValues());
                matchRuleRecord.setExclusionCriterionList(matchRule.getExclusionCriterionList());
                matchRuleRecord.setAcceptCriterion(matchRule.getAcceptCriterion());
                matchRuleRecord.setReviewCriterion(matchRule.getReviewCriterion());

                matchRuleRecord.setMatchRuleId(generateRandomMatchRuleId());
                matchRuleRecord.setVersionId(1);
                matchRuleRecord.setState(MatchRuleRecord.State.ACTIVE);
                matchRuleRecord.setTenant(MultiTenantContext.getTenant());

                matchRuleEntityMgr.create(matchRuleRecord);
                return convertMatchRuleRecord(matchRuleRecord);
            } finally {
                redisDistributedLock.releaseLock(lockKey, requestId);
            }
        }
        throw new RuntimeException("Cannot get create lock, please try again later.");
    }

    @Override
    public List<MatchRule> getMatchRuleList(String customerSpace, String sourceId, Boolean includeArchived, Boolean includeInactive) {
        List<MatchRuleRecord.State> states = new ArrayList<>();
        states.add(MatchRuleRecord.State.ACTIVE);
        if (Boolean.TRUE.equals(includeInactive)) {
            states.add(MatchRuleRecord.State.INACTIVE);
        }
        if (Boolean.TRUE.equals(includeArchived)) {
            states.add(MatchRuleRecord.State.ARCHIVED);
        }
        List<MatchRuleRecord> matchRuleRecords;
        if (states.size() == 1) {
            matchRuleRecords = matchRuleEntityMgr.findMatchRules(sourceId, states.get(0));
        } else {
            matchRuleRecords = matchRuleEntityMgr.findMatchRules(sourceId, states);
        }
        if (CollectionUtils.isEmpty(matchRuleRecords)) {
            return Collections.emptyList();
        } else {
            return matchRuleRecords.stream().map(this::convertMatchRuleRecord).collect(Collectors.toList());
        }
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

    @Override
    public void hardDeleteMatchRuleBySourceId(String customerSpace, String sourceId) {
        List<MatchRuleRecord.State> states = new ArrayList<>();
        states.add(MatchRuleRecord.State.ACTIVE);
        states.add(MatchRuleRecord.State.INACTIVE);
        states.add(MatchRuleRecord.State.ARCHIVED);
        List<MatchRuleRecord> matchRuleRecords;
        matchRuleRecords = matchRuleEntityMgr.findMatchRules(sourceId, states);
        if (CollectionUtils.isNotEmpty(matchRuleRecords)) {
            matchRuleRecords.forEach(matchRuleRecord -> {
                matchRuleEntityMgr.delete(matchRuleRecord);
            });
        }
    }

    private MatchRule convertMatchRuleRecord(MatchRuleRecord record) {
        MatchRule matchRule = new MatchRule();
        matchRule.setSourceId(record.getSourceId());
        matchRule.setDisplayName(record.getDisplayName());
        matchRule.setRuleType(record.getRuleType());
        matchRule.setMatchKey(record.getMatchKey());
        matchRule.setDomain(record.getDomain());
        matchRule.setRecordType(record.getRecordType());
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
