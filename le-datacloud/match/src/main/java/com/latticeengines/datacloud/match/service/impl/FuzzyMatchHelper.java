package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.newrelic.api.agent.Trace;

import scala.concurrent.Future;

@Component("fuzzyMatchHelper")
public class FuzzyMatchHelper implements DbHelper {

    private static final Log log = LogFactory.getLog(FuzzyMatchHelper.class);

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    @Qualifier("accountMasterColumnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @Autowired
    private FuzzyMatchService fuzzyMatchService;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

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

    private void fetchInternal(MatchContext context, boolean isSync) {
        boolean fetchOnly = context.getInput().getFetchOnly();
        if (!fetchOnly) {
            try {
                updateDecisionGraph(context.getInput());
                updateUseRemoteDnB(context.getInput());
                updateBypassDnBCache(context.getInput());
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
        if (matchInput.getTenant() != null) {
            CustomerSpace customerSpace = CustomerSpace.parse(matchInput.getTenant().getId());
            matchInput.setUseRemoteDnB(zkConfigurationService.fuzzyMatchEnabled(customerSpace));
        } else {
            matchInput.setUseRemoteDnB(false);
        }
    }

    private void updateBypassDnBCache(MatchInput matchInput) {
        if (matchInput.getTenant() != null) {
            CustomerSpace customerSpace = CustomerSpace.parse(matchInput.getTenant().getId());
            boolean bypassDnBCache = zkConfigurationService.bypassDnBCache(customerSpace);
            if (bypassDnBCache) {
                matchInput.setUseDnBCache(false);
            }
        }
    }

    @Override
    public MatchContext fetchAsync(MatchContext context) {
        fetchInternal(context, false);
        return context;
    }

    @Override
    public void fetchIdResult(MatchContext context) {
        try {
            fuzzyMatchService.fetchIdResult(context.getInternalResults(), context.getInput().getLogLevel(),
                    context.getFuturesResult());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to process match result!", ex);
        }
    }

    @Override
    public void fetchMatchResult(MatchContext context) {
        String dataCloudVersion = context.getInput().getDataCloudVersion();
        boolean idOnly = context.isSeekingIdOnly();
        if (!idOnly) {
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
        boolean latticeAccountIdOnly = context.isSeekingIdOnly();
        if (!latticeAccountIdOnly) {
            for (InternalOutputRecord record : context.getInternalResults()) {
                updateInternalRecordByMatchedAccount(record, context.getColumnSelection(), context.getInput()
                        .getDataCloudVersion());
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
        Map<String, Object> queryResult = parseLatticeAccount(record.getLatticeAccount(), columnSelection,
                dataCloudVersion);
        record.setQueryResult(queryResult);
    }

    @Trace
    private Map<String, Object> parseLatticeAccount(LatticeAccount account, ColumnSelection columnSelection,
            String dataCloudVersion) {
        Map<String, Pair<BitCodeBook, List<String>>> parameters = columnSelectionService.getDecodeParameters(
                columnSelection, dataCloudVersion);
        Map<String, Object> queryResult = new HashMap<>();
        Map<String, Object> amAttributes = (account == null) ? new HashMap<>() : account.getAttributes();
        amAttributes.put(MatchConstants.LID_FIELD, (account == null) ? null : account.getId());

        Set<String> attrMask = new HashSet<>();
        attrMask.addAll(columnSelection.getColumnIds());
        Map<String, Object> decodedAttributes = decodeAttributes(parameters, amAttributes, attrMask);
        for (Column column : columnSelection.getColumns()) {
            String columnId = column.getExternalColumnId();
            String columnName = column.getColumnName();
            if (amAttributes.containsKey(columnId)) {
                queryResult.put(columnName, amAttributes.get(columnId));
            } else if (decodedAttributes.containsKey(columnId)) {
                queryResult.put(columnName, decodedAttributes.get(columnId));
            } else {
                queryResult.put(columnName, null);
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
        Map<String, Object> amAttributes = (record.getLatticeAccount() == null) ? new HashMap<>() : record
                .getLatticeAccount().getAttributes();
        amAttributes.put(MatchConstants.LID_FIELD, (record.getLatticeAccount() == null) ? null : record
                .getLatticeAccount().getId());

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
        nameLocation.setCity(country);
        nameLocation.setZipcode(zipCode);
        nameLocation.setPhoneNumber(phone);
        record.setMatchedNameLocation(nameLocation);
        record.setMatchedDuns((String) amAttributes.get(MatchConstants.AM_DUNS_FIELD));
        record.setMatchedDduns((String) amAttributes.get(MatchConstants.AM_DDUNS_FIELD));
    }

}
