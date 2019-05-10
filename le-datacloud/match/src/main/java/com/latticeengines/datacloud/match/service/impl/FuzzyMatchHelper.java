package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.CDLLookupService;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

import scala.concurrent.Future;

@Component("fuzzyMatchHelper")
public class FuzzyMatchHelper implements DbHelper {

    private static final Logger log = LoggerFactory.getLogger(FuzzyMatchHelper.class);

    @Inject
    private AccountLookupService accountLookupService;

    @Resource(name = "accountMasterColumnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @Inject
    private FuzzyMatchService fuzzyMatchService;

    @Inject
    private ZkConfigurationService zkConfigurationService;

    @Inject
    private CDLLookupService cdlLookupService;

    @Inject
    private EntityMatchInternalService entityMatchInternalService;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    public MatchContext sketchExecutionPlan(MatchContext matchContext, boolean skipExecutionPlanning) {
        return matchContext;
    }

    @Override
    public void initExecutors() {
    }

    @Override
    public MatchContext fetch(MatchContext context) {
        fetchInternal(context, true);
        fetchMatchResult(context);
        return context;
    }

    @MatchStep
    private void fetchInternal(MatchContext context, boolean isSync) {
        boolean fetchOnly = context.getInput().isFetchOnly();
        if (!fetchOnly) {
            try {
                updateDecisionGraph(context.getInput());
                updateUseRemoteDnB(context.getInput());
                if (Boolean.TRUE.equals(context.isCdlLookup())) {
                    preLookup(context);
                }
                if (isSync) {
                    fuzzyMatchService.callMatch(context.getInternalResults(), context.getInput());
                } else {
                    List<Future<Object>> futures = fuzzyMatchService.callMatchAsync(context.getInternalResults(),
                            context.getInput());
                    context.setFuturesResult(futures);
                }
            } catch (Exception e) {
                log.error("Failed to run fuzzy match.", e);
            }
        }
    }

    private void preLookup(MatchContext context) {
        for (InternalOutputRecord record : context.getInternalResults()) {
            String lookupIdKey = record.getLookupIdKey();
            String lookupIdValue = record.getLookupIdValue();
            if (StringUtils.isNotBlank(lookupIdValue)) {
                List<DynamoDataUnit> dynamoDataUnits = context.getCustomDataUnits();
                Map<String, Object> customAccount = cdlLookupService.lookup(dynamoDataUnits, lookupIdKey,
                        lookupIdValue);
                if (MapUtils.isNotEmpty(customAccount)) {
                    if (InterfaceName.AccountId.name().equals(record.getLookupIdKey())) {
                        record.setEntityIds(new HashMap<>());
                        record.getEntityIds().put(BusinessEntity.Account.name(), record.getLookupIdValue());
                    }
                    record.setCustomAccount(customAccount);
                    String latticeAccountId = (String) customAccount.get(InterfaceName.LatticeAccountId.name());
                    if (StringUtils.isNotBlank(latticeAccountId)) {
                        String outputId = StringStandardizationUtils.getStandardizedOutputLatticeID(latticeAccountId);
                        record.setMatchedLatticeAccountId(outputId);
                        String inputId = StringStandardizationUtils.getStandardizedInputLatticeID(latticeAccountId);
                        record.setLatticeAccountId(inputId);
                    }
                } else {
                    record.setMatched(false);
                    String msg = "Cannot find a custom account by " + lookupIdKey + "=" + lookupIdValue;
                    record.addErrorMessages(msg);
                    log.warn(msg);
                }
            }
        }
    }

    private void updateDecisionGraph(MatchInput matchInput) {
        String decisionGraph = matchInput.getDecisionGraph();
        if (StringUtils.isEmpty(decisionGraph) && matchInput.getTenant() != null) {
            decisionGraph = defaultGraph;
        }
        matchInput.setDecisionGraph(decisionGraph);
    }

    private void updateUseRemoteDnB(MatchInput matchInput) {
        if (!zkConfigurationService.useRemoteDnBGlobal()) {
            matchInput.setUseRemoteDnB(Boolean.FALSE);
            return;
        }
        Boolean useRemoteDnB = matchInput.getUseRemoteDnB();
        if (useRemoteDnB != null) {
            return;
        }
        matchInput.setUseRemoteDnB(true);
    }

    @Override
    public MatchContext fetchAsync(MatchContext context) {
        fetchInternal(context, false);
        return context;
    }

    @Override
    public void fetchIdResult(MatchContext context) {
        try {
            fuzzyMatchService.fetchIdResult(context.getInternalResults(), context.getInput().getLogLevelEnum(),
                    context.getFuturesResult());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to process match result!", ex);
        }
    }

    @MatchStep
    @Override
    public void fetchMatchResult(MatchContext context) {
        if (!context.isSeekingIdOnly()) {
            if (OperationalMode.ENTITY_MATCH.equals(context.getInput().getOperationalMode())) {
                fetchEntityMatchResult(context);
            } else {
                fetchLDCMatchResult(context);
            }
        }
    }

    private void fetchEntityMatchResult(MatchContext context) {
        List<String> seedIds = context.getInternalResults().stream() //
                .map(InternalOutputRecord::getEntityId) //
                .collect(Collectors.toList());
        Long startTime = System.currentTimeMillis();
        Tenant tenant = new Tenant(CustomerSpace.parse(context.getInput().getTenant().getId()).getTenantId());
        List<EntityRawSeed> seeds = entityMatchInternalService.get(tenant, context.getInput().getTargetEntity(),
                seedIds);
        int unmatch = 0;
        for (int i = 0; i < context.getInternalResults().size(); i++) {
            InternalOutputRecord record = context.getInternalResults().get(i);
            EntityRawSeed seed = seeds.get(i);
            if (seed == null) {
                unmatch++;
                continue;
            }
            // As long as EntityId exists, treat it as matched even if seed
            // attributes are not (fully) populated
            record.setMatched(true);
            if (CollectionUtils.isNotEmpty(seed.getLookupEntries())) {
                EntityLookupEntry lookupEntry = seed.getLookupEntries().stream() //
                        .filter(entry -> EntityLookupEntry.Type.EXTERNAL_SYSTEM.equals(entry.getType())
                                && InterfaceName.AccountId.name().equals(entry.getSerializedKeys())) //
                        .findFirst() //
                        .orElse(null);
                if (lookupEntry != null) {
                    record.getQueryResult().put(InterfaceName.AccountId.name(),
                            lookupEntry.getSerializedValues());
                }
            }
            if (MapUtils.isNotEmpty(seed.getAttributes())) {
                String latticeAccountId = seed.getAttributes().get(InterfaceName.LatticeAccountId.name());
                record.getQueryResult().put(InterfaceName.LatticeAccountId.name(), latticeAccountId);
                record.setLatticeAccountId(latticeAccountId);
            }
        }
        log.info("Fetched records from entity seed table by entity id: Total={}, Unmatched={}, Duration={}",
                context.getInternalResults().size(), unmatch, System.currentTimeMillis() - startTime);
    }

    private void fetchLDCMatchResult(MatchContext context) {
        String dataCloudVersion = context.getInput().getDataCloudVersion();
        List<String> ids = new ArrayList<>();
        int notNullIds = 0;
        for (InternalOutputRecord record : context.getInternalResults()) {
            String latticeAccountId = record.getLatticeAccountId();
            ids.add(latticeAccountId);
            if (StringUtils.isNotEmpty(latticeAccountId)) {
                notNullIds++;
            }
        }
        Long startTime = System.currentTimeMillis();
        List<LatticeAccount> accounts = accountLookupService.batchFetchAccounts(ids, dataCloudVersion);

        for (int i = 0; i < ids.size(); i++) {
            InternalOutputRecord record = context.getInternalResults().get(i);
            LatticeAccount account = accounts.get(i);
            record.setLatticeAccount(account);
        }

        if (notNullIds > 0) {
            log.info(String.format("Fetched %d accounts from dynamodb. Duration=%d Rows=%d", accounts.size(),
                    System.currentTimeMillis() - startTime, accounts.size()));
        }
    }

    @Override
    public MatchContext fetchSync(MatchContext context) {
        return fetch(context);
    }

    @Override
    public List<MatchContext> fetch(List<MatchContext> contexts) {
        if (contexts.isEmpty()) {
            return Collections.emptyList();
        }

        log.info("Enter executeBulk for " + contexts.size() + " match contexts.");

        String dataCloudVersion = contexts.get(0).getInput().getDataCloudVersion();
        MatchContext mergedContext = mergeContexts(contexts, dataCloudVersion);

        mergedContext = fetch(mergedContext);
        splitContext(mergedContext, contexts);

        return contexts;
    }

    @Override
    public MatchContext updateInternalResults(MatchContext context) {
        if (!context.isSeekingIdOnly()
                && !OperationalMode.ENTITY_MATCH.equals(context.getInput().getOperationalMode())) {
            // FOR LDC match: Use record.latticeAccount to update
            // record.queryResult
            // For entity match: record.queryResult is already prepared in
            // fetchEntityMatchResult(), so skip this part
            for (InternalOutputRecord record : context.getInternalResults()) {
                updateInternalRecordByMatchedAccount(record, context.getColumnSelection(),
                        context.getInput().getDataCloudVersion());
                if (record.isMatched()) {
                    setMatchedValues(record);
                }
            }
        }
        return context;
    }

    @Override
    public MatchContext mergeContexts(List<MatchContext> matchContextList, String dataCloudVersion) {
        MatchContext mergedContext = new MatchContext();
        mergedContext.setInput(matchContextList.get(0).getInput());
        mergedContext.setMatchEngine(matchContextList.get(0).getMatchEngine());
        List<InternalOutputRecord> internalOutputRecords = new ArrayList<>();
        for (MatchContext matchContext : matchContextList) {
            String contextId = UUID.randomUUID().toString();
            matchContext.setContextId(contextId);
            for (InternalOutputRecord record : matchContext.getInternalResults()) {
                record.setOriginalContextId(contextId);
                internalOutputRecords.add(record);
            }
        }
        mergedContext.setInternalResults(internalOutputRecords);
        return mergedContext;
    }

    @Override
    public void splitContext(MatchContext mergedContext, List<MatchContext> matchContextList) {
        Map<String, MatchContext> rootUidContextMap = new HashMap<>();
        for (MatchContext context : matchContextList) {
            rootUidContextMap.put(context.getContextId(), context);
            context.setInternalResults(new ArrayList<>());
        }
        for (InternalOutputRecord internalOutputRecord : mergedContext.getInternalResults()) {
            MatchContext originalContext = rootUidContextMap.get(internalOutputRecord.getOriginalContextId());
            originalContext.getInternalResults().add(internalOutputRecord);
        }
    }

    private void updateInternalRecordByMatchedAccount(InternalOutputRecord record, ColumnSelection columnSelection,
            String dataCloudVersion) {
        Map<String, Object> queryResult = new HashMap<>();
        Map<String, Object> latticeAccount = parseLatticeAccount(record.getLatticeAccount(), columnSelection,
                dataCloudVersion);
        if (MapUtils.isNotEmpty(latticeAccount)) {
            queryResult.putAll(latticeAccount);
        }
        if (MapUtils.isNotEmpty(record.getCustomAccount())) {
            queryResult.putAll(record.getCustomAccount());
        }
        if (record.getLatticeAccount() != null && record.getLatticeAccount().getId() != null) {
            record.setMatched(true);
        }
        record.setQueryResult(queryResult);
    }

    private Map<String, Object> parseLatticeAccount(LatticeAccount account, ColumnSelection columnSelection,
            String dataCloudVersion) {
        Map<String, Pair<BitCodeBook, List<String>>> parameters = columnSelectionService
                .getDecodeParameters(columnSelection, dataCloudVersion);
        Map<String, Object> queryResult = new HashMap<>();
        Map<String, Object> amAttributes = (account == null) ? new HashMap<>() : account.getAttributes();
        amAttributes.put(MatchConstants.LID_FIELD, (account == null) ? null : account.getId());

        Set<String> attrMask = new HashSet<>(columnSelection.getColumnIds());
        Map<String, Object> decodedAttributes = decodeAttributes(parameters, amAttributes, attrMask);
        for (Column column : columnSelection.getColumns()) {
            String columnId = column.getExternalColumnId();
            String columnName = column.getColumnName();
            if (amAttributes.containsKey(columnId)) {
                queryResult.put(columnName, amAttributes.get(columnId));
            } else {
                Object value = decodedAttributes.getOrDefault(columnId, null);
                queryResult.put(columnName, value);
            }
        }

        return queryResult;
    }

    private Map<String, Object> decodeAttributes(Map<String, Pair<BitCodeBook, List<String>>> parameters,
            Map<String, Object> amAttributes, Set<String> attrMask) {
        Map<String, Object> decodedAttributes = new HashMap<>();
        for (Map.Entry<String, Pair<BitCodeBook, List<String>>> entry : parameters.entrySet()) {
            BitCodeBook codeBook = entry.getValue().getLeft();
            List<String> decodeFields = entry.getValue().getRight();
            String encodeField = entry.getKey();
            String encodedStr = (String) amAttributes.get(encodeField);
            decodeFields.retainAll(attrMask);
            if (!decodeFields.isEmpty()) {
                decodedAttributes.putAll(codeBook.decode(encodedStr, decodeFields));
            }
        }
        return decodedAttributes;
    }

    private void setMatchedValues(InternalOutputRecord record) {
        Map<String, Object> amAttributes = (record.getLatticeAccount() == null) ? new HashMap<>()
                : record.getLatticeAccount().getAttributes();
        amAttributes.put(MatchConstants.LID_FIELD,
                (record.getLatticeAccount() == null) ? null : record.getLatticeAccount().getId());

        record.setLatticeAccountId((String) amAttributes.get(MatchConstants.LID_FIELD));
        record.setMatchedDomain((String) amAttributes.get(MatchConstants.AM_DOMAIN_FIELD));
        record.setMatchedEmployeeRange((String) amAttributes.get(MatchConstants.AM_EMPLOYEE_RANGE_FIELD));
        record.setMatchedRevenueRange((String) amAttributes.get(MatchConstants.AM_REVENUE_RANGE_FIELD));
        record.setMatchedPrimaryIndustry((String) amAttributes.get(MatchConstants.AM_PRIMARY_INDUSTRY_FIELD));
        record.setMatchedSecondIndustry((String) amAttributes.get(MatchConstants.AM_SECOND_INDUSTRY_FIELD));
        record.setDomainSource((String) amAttributes.get(MatchConstants.AM_DOMAIN_SOURCE));

        String name = (String) amAttributes.get(MatchConstants.AM_NAME_FIELD);
        String city = (String) amAttributes.get(MatchConstants.AM_CITY_FIELD);
        String state = (String) amAttributes.get(MatchConstants.AM_STATE_FIELD);
        String country = (String) amAttributes.get(MatchConstants.AM_COUNTRY_FIELD);
        String zipCode = (String) amAttributes.get(MatchConstants.AM_ZIPCODE_FIELD);
        String phone = (String) amAttributes.get(MatchConstants.AM_PHONE_NUM_FIELD);

        NameLocation nameLocation = new NameLocation();
        nameLocation.setName(name);
        nameLocation.setCity(city);
        nameLocation.setState(state);
        nameLocation.setCountry(country);
        nameLocation.setZipcode(zipCode);
        nameLocation.setPhoneNumber(phone);
        record.setMatchedNameLocation(nameLocation);
        record.setMatchedDuns((String) amAttributes.get(MatchConstants.AM_DUNS_FIELD));
        record.setMatchedDduns((String) amAttributes.get(MatchConstants.AM_DDUNS_FIELD));
    }

}
