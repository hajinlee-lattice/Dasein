package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.DbHelper;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;

@Component("dynamoDbHelper")
public class DynamoDbHelper implements DbHelper {

    private static final Log log = LogFactory.getLog(DynamoDbHelper.class);

    @Autowired
    private AccountLookupService accountLookupService;

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForAccountMasterBasedMatch(version);
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
    public MatchContext fetch(MatchContext context) {
        AccountLookupRequest request = context.getAccountLookupRequest();
        if (request == null) {
            throw new NullPointerException("Cannot find AccountLookupRequest in the MatchContext");
        }
        Long startTime = System.currentTimeMillis();
        List<LatticeAccount> accounts = accountLookupService.batchLookup(request);
        context.setMatchedAccounts(accounts);
        log.info(String.format("Fetched %d accounts from dynamodb. Duration=%d Rows=%d", accounts.size(),
                System.currentTimeMillis() - startTime, accounts.size()));
        return context;
    }

    @Override
    public MatchContext updateInternalResults(MatchContext context) {
        List<LatticeAccount> accountsInOrder = context.getMatchedAccounts();
        List<InternalOutputRecord> internalRecordsInOrder = context.getInternalResults();
        for (int i = 0; i < internalRecordsInOrder.size(); i++) {
            LatticeAccount matchedAccount = accountsInOrder.get(i);
            InternalOutputRecord internalOutputRecord = internalRecordsInOrder.get(i);
            updateInternalRecordByMatchedAccount(internalOutputRecord, matchedAccount, context.getColumnSelection());
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
            for (String id: allIds) {
                request.addId(id);
            }
            mergedContext.setAccountLookupRequest(request);
        }
        return mergedContext;
    }


    @Override
    public void splitContext(MatchContext mergedContext, List<MatchContext> matchContextList) {
        Map<String, LatticeAccount> allAccounts = new HashMap<>();
        for (LatticeAccount account: mergedContext.getMatchedAccounts()) {
            allAccounts.put(account.getId(), account);
        }
        for (MatchContext context : matchContextList) {
            List<String> idsInOrder = context.getAccountLookupRequest().getIds();
            List<LatticeAccount> accountsInOrder = new ArrayList<>();
            for (String id: idsInOrder) {
                accountsInOrder.add(allAccounts.get(id));
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
                                                      ColumnSelection columnSelection) {
        List<Column> columns = columnSelection.getColumns();

        Map<String, Object> queryResult = new HashMap<>();
        Map<String, Object> amAttributes = (account == null) ? new HashMap<String, Object>() : account.getAttributes();
        for (Column column: columns) {
            String columnName = column.getColumnName();
            if (amAttributes.containsKey(columnName)) {
                queryResult.put(columnName, amAttributes.get(columnName));
            } else {
                queryResult.put(columnName, null);
            }
        }
        record.setQueryResult(queryResult);
    }

}
