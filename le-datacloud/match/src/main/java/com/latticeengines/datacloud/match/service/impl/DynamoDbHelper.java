package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.DbHelper;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.newrelic.api.agent.Trace;

@Component("dynamoDbHelper")
public class DynamoDbHelper implements DbHelper {

    private static final Log log = LogFactory.getLog(DynamoDbHelper.class);

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    @Qualifier("accountMasterColumnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @Value("${datacloud.match.use.fuzzy.match:false}")
    private boolean useFuzzyMatch;

    @Override
    public boolean accept(String version) {
        return !useFuzzyMatch && MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    public void populateMatchHints(MatchContext context) {
        List<Triple<InternalOutputRecord, AccountLookupRequest, MatchContext>> lookupRequestTriplets = new ArrayList<>();
        AccountLookupRequest request = createLookupRequest(context, lookupRequestTriplets);
        context.setAccountLookupRequest(request);
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
        return fetchSync(context);
    }

    @Override
    public MatchContext fetchSync(MatchContext context) {
        AccountLookupRequest request = context.getAccountLookupRequest();
        if (request == null) {
            throw new NullPointerException("Cannot find AccountLookupRequest in the MatchContext");
        }
        Long startTime = System.currentTimeMillis();
        List<String> lookupIdsInOrder = request.getIds();
        List<LatticeAccount> accounts = accountLookupService.batchLookup(request);
        Map<String, String> latticeIdToLookupIdMap = new HashMap<>();
        for (int i = 0; i < accounts.size(); i++) {
            String lookupId = lookupIdsInOrder.get(i);
            LatticeAccount account = accounts.get(i);
            if (account != null) {
                latticeIdToLookupIdMap.put(account.getId(), lookupId);
            }
        }
        context.setMatchedAccounts(accounts);
        context.setLatticeIdToLookupIdMap(latticeIdToLookupIdMap);
        log.info(String.format("Fetched %d accounts from dynamodb. Duration=%d Rows=%d", accounts.size(),
                System.currentTimeMillis() - startTime, accounts.size()));
        return context;
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
        List<LatticeAccount> accountsInOrder = context.getMatchedAccounts();
        List<InternalOutputRecord> internalRecordsInOrder = context.getInternalResults();
        for (int i = 0; i < internalRecordsInOrder.size(); i++) {
            LatticeAccount matchedAccount = accountsInOrder.get(i);
            InternalOutputRecord internalOutputRecord = internalRecordsInOrder.get(i);
            updateInternalRecordByMatchedAccount(internalOutputRecord, matchedAccount, context.getColumnSelection(),
                    context.getInput().getDataCloudVersion());
        }
        context.setInternalResults(internalRecordsInOrder);
        return context;
    }

    @Override
    public MatchContext mergeContexts(List<MatchContext> matchContextList, String dataCloudVersion) {
        MatchContext mergedContext = new MatchContext();
        MatchInput dummyInput = new MatchInput();
        dummyInput.setDataCloudVersion(dataCloudVersion);
        mergedContext.setInput(dummyInput);
        Set<String> allIds = new HashSet<>();
        for (MatchContext matchContext : matchContextList) {
            AccountLookupRequest request = matchContext.getAccountLookupRequest();
            if (request != null) {
                allIds.addAll(request.getIds());
            }
        }
        if (!allIds.isEmpty()) {
            AccountLookupRequest request = new AccountLookupRequest(dataCloudVersion);
            for (String id : allIds) {
                request.addId(id);
            }
            mergedContext.setAccountLookupRequest(request);
        }
        return mergedContext;
    }

    @Override
    public void splitContext(MatchContext mergedContext, List<MatchContext> matchContextList) {
        Map<String, LatticeAccount> allAccounts = new HashMap<>();
        Map<String, String> latticeIdToLookupIdMap = mergedContext.getLatticeIdToLookupIdMap();
        for (LatticeAccount account : mergedContext.getMatchedAccounts()) {
            if (account != null) {
                String latticeId = latticeIdToLookupIdMap.get(account.getId());
                allAccounts.put(latticeId, account);
            }
        }
        for (MatchContext context : matchContextList) {
            List<String> lookupIdsInOrder = context.getAccountLookupRequest().getIds();
            List<LatticeAccount> accountsInOrder = new ArrayList<>();
            for (String id : lookupIdsInOrder) {
                if (allAccounts.containsKey(id)) {
                    accountsInOrder.add(allAccounts.get(id));
                } else {
                    accountsInOrder.add(null);
                }
            }
            context.setMatchedAccounts(accountsInOrder);
        }
    }

    private AccountLookupRequest createLookupRequest(MatchContext matchContext,
            List<Triple<InternalOutputRecord, AccountLookupRequest, MatchContext>> lookupRequestTriplets) {
        String dataCloudVersion = matchContext.getInput().getDataCloudVersion();
        AccountLookupRequest accountLookupRequest = new AccountLookupRequest(dataCloudVersion);
        populateLookupRequest(matchContext, lookupRequestTriplets, accountLookupRequest);
        return accountLookupRequest;
    }

    private void populateLookupRequest(MatchContext matchContext,
            List<Triple<InternalOutputRecord, AccountLookupRequest, MatchContext>> lookupRequestTriplets,
            AccountLookupRequest accountLookupRequest) {

        for (InternalOutputRecord record : matchContext.getInternalResults()) {
            accountLookupRequest.addLookupPair(
                    (StringUtils.isEmpty(record.getParsedDomain())
                            || "null".equalsIgnoreCase(record.getParsedDomain().trim()) || record.isPublicDomain())
                                    ? null : record.getParsedDomain(),
                    (StringUtils.isEmpty(record.getParsedDuns())
                            || "null".equalsIgnoreCase(record.getParsedDuns().trim()) ? null : record.getParsedDuns()));
            Triple<InternalOutputRecord, AccountLookupRequest, MatchContext> accountLookupRequestTriplet = new MutableTriple<>(
                    record, accountLookupRequest, matchContext);
            lookupRequestTriplets.add(accountLookupRequestTriplet);
        }
    }

    private void updateInternalRecordByMatchedAccount(InternalOutputRecord record, LatticeAccount account,
            ColumnSelection columnSelection, String dataCloudVersion) {
        Map<String, Object> queryResult = parseLatticeAccount(account, columnSelection, dataCloudVersion);
        record.setQueryResult(queryResult);
    }

    @Trace
    private Map<String, Object> parseLatticeAccount(LatticeAccount account,
                                                    ColumnSelection columnSelection, String dataCloudVersion) {
        Map<String, Pair<BitCodeBook, List<String>>> parameters = columnSelectionService
                .getDecodeParameters(columnSelection, dataCloudVersion);

        Map<String, Object> queryResult = new HashMap<>();
        Map<String, Object> amAttributes = (account == null) ? new HashMap<String, Object>() : account.getAttributes();
        for (Column column : columnSelection.getColumns()) {
            String columnName = column.getColumnName();

            Map<String, Object> decodedAttributes = new HashMap<>();
            for (Map.Entry<String, Pair<BitCodeBook, List<String>>> entry: parameters.entrySet()) {
                BitCodeBook codeBook = entry.getValue().getLeft();
                List<String> decodeFields = entry.getValue().getRight();
                String encodeField = entry.getKey();
                String encodedStr = (String) amAttributes.get(encodeField);
                decodedAttributes.putAll(codeBook.decode(encodedStr, decodeFields));
            }

            if (amAttributes.containsKey(columnName)) {
                queryResult.put(columnName, amAttributes.get(columnName));
            } else if (decodedAttributes.containsKey(columnName)) {
                queryResult.put(columnName, decodedAttributes.get(columnName));
            } else {
                queryResult.put(columnName, null);
            }
        }
        return queryResult;
    }

}
