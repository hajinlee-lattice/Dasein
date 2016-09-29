package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component("realTimeMatchWithAccountMasterService")
public class RealTimeMatchWithAccountMasterServiceImpl extends RealTimeMatchWithDerivedColumnCacheServiceImpl {

    private static final String DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING = "2.";

    @Autowired
    private AccountLookupService accountLookupService;

    @Override
    public boolean accept(String version) {
        if (!StringUtils.isEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING)) {
            return true;
        }

        return false;
    }

    @Override
    @MatchStep(threshold = 0L)
    public MatchOutput match(MatchInput input) {
        MatchContext matchContext = prepareMatchContext(input, null, true);
        Date receivedAt = new Date();
        String rootOperationUID = UUID.randomUUID().toString();

        List<Triple<InternalOutputRecord, AccountLookupRequest, MatchContext>> lookupRequestTriplets = new ArrayList<>();
        AccountLookupRequest request = createLookupRequest(matchContext, lookupRequestTriplets);

        if (request.getIds().size() < lookupRequestTriplets.size()) {
            // TODO anoop - in M6 - add the logic for external lookup based on
            // namelocation from DnB matcher
            doNameLocationBasedLookup(input, matchContext, lookupRequestTriplets);
        }

        // TODO - anoop - currently assuming happy path - handle other scenarios
        // in next txn
        List<LatticeAccount> matchedResults = accountLookupService.batchLookup(request);

        MatchOutput output = createMatchOutput(input, matchContext, receivedAt, rootOperationUID, matchedResults,
                lookupRequestTriplets);

        return output;
    }

    @Override
    @MatchStep(threshold = 0L)
    public BulkMatchOutput matchBulk(BulkMatchInput input) {
        String rootOperationUID = UUID.randomUUID().toString();
        input.setRequestId(rootOperationUID);

        BulkMatchOutput output = new BulkMatchOutput();
        List<MatchOutput> outputList = new ArrayList<>();
        output.setOutputList(outputList);

        List<MatchContext> matchContexts = new ArrayList<>();

        // prepare match contexts
        for (MatchInput singleInput : input.getInputList()) {
            MatchContext matchContext = prepareMatchContext(singleInput, null, true);
            matchContexts.add(matchContext);
        }

        List<Triple<InternalOutputRecord, AccountLookupRequest, MatchContext>> lookupRequestTriplets = new ArrayList<>();

        AccountLookupRequest bulkAccountLookupRequest = createBulkLookupRequest(matchContexts, lookupRequestTriplets);

        List<LatticeAccount> matchedResults = accountLookupService.batchLookup(bulkAccountLookupRequest);
        int idx = 0;
        int idxInRunningMatchContext = 0;
        MatchContext runningMatchContext = null;
        for (LatticeAccount result : matchedResults) {

            MatchContext matchContext = lookupRequestTriplets.get(idx++).getRight();

            if (runningMatchContext != matchContext) {
                runningMatchContext = matchContext;
                idxInRunningMatchContext = 0;
            }

            MatchOutput matchOutput = matchContext.getOutput();

            if (matchOutput.getResult() == null) {
                matchOutput.setResult(new ArrayList<OutputRecord>());
            }

            List<OutputRecord> outputRecordList = matchOutput.getResult();

            List<Column> selectedColumns = matchContext.getColumnSelection().getColumns();
            OutputRecord outputRecord = createOutputRecord(selectedColumns, result, idxInRunningMatchContext++);
            outputRecordList.add(outputRecord);
            outputList.add(matchOutput);
        }

        return output;
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
            // we can not do direct lookup against account master if duns is not
            // specified and no valid domain specified
            boolean shouldCallExternalMatch = (StringUtils.isEmpty(record.getParsedDomain()) || record.isPublicDomain())
                    && StringUtils.isEmpty(record.getParsedDuns());

            if (!shouldCallExternalMatch) {
                accountLookupRequest.addLookupPair(
                        (StringUtils.isEmpty(record.getParsedDomain())
                                || "null".equalsIgnoreCase(record.getParsedDomain().trim()) || record.isPublicDomain())
                                        ? null : record.getParsedDomain(),
                        (StringUtils.isEmpty(record.getParsedDuns())
                                || "null".equalsIgnoreCase(record.getParsedDuns().trim()) ? null
                                        : record.getParsedDuns()));
            }

            Triple<InternalOutputRecord, AccountLookupRequest, MatchContext> accountLookupRequestTriplet = new MutableTriple<>(
                    record, accountLookupRequest, matchContext);
            lookupRequestTriplets.add(accountLookupRequestTriplet);
        }
    }

    private AccountLookupRequest createBulkLookupRequest(List<MatchContext> matchContexts,
            List<Triple<InternalOutputRecord, AccountLookupRequest, MatchContext>> lookupRequestTriplets) {
        String dataCloudVersion = matchContexts.get(0).getInput().getDataCloudVersion();
        AccountLookupRequest accountLookupRequest = new AccountLookupRequest(dataCloudVersion);

        for (MatchContext matchContext : matchContexts) {
            populateLookupRequest(matchContext, lookupRequestTriplets, accountLookupRequest);
        }

        return accountLookupRequest;
    }

    private MatchOutput createMatchOutput(MatchInput input, MatchContext matchContext, Date receivedAt,
            String rootOperationUID, List<LatticeAccount> matchedResults,
            List<Triple<InternalOutputRecord, AccountLookupRequest, MatchContext>> lookupRequestTriplets) {
        List<OutputRecord> outputRecords = new ArrayList<>();
        MatchOutput output = matchContext.getOutput();
        output.setResult(outputRecords);
        ColumnSelection columnSelection = matchContext.getColumnSelection();
        List<Column> selectedColumns = columnSelection.getColumns();
        int index = 0;

        for (Triple<InternalOutputRecord, AccountLookupRequest, MatchContext> triplet : lookupRequestTriplets) {
            if (triplet.getMiddle() == null) {
                outputRecords.add(triplet.getLeft());
            } else {
                LatticeAccount result = matchedResults.get(index++);

                OutputRecord outputRecord = triplet.getLeft();
                outputRecord.setMatched(result != null);
                outputRecord.setOutput(
                        outputRecord.isMatched() ? getAttributeValues(selectedColumns, result.getAttributes()) : null);

                outputRecords.add(outputRecord);
            }
        }

        return output;
    }

    private OutputRecord createOutputRecord(List<Column> selectedColumns, LatticeAccount result, Integer rowNumber) {
        OutputRecord outputRecord = new OutputRecord();
        outputRecord.setMatched(result != null);
        outputRecord.setOutput(
                outputRecord.isMatched() ? getAttributeValues(selectedColumns, result.getAttributes()) : null);
        outputRecord.setRowNumber(rowNumber);
        return outputRecord;
    }

    private List<Object> getAttributeValues(List<Column> selectedColumns, Map<String, Object> attributes) {
        List<Object> attributeValues = new ArrayList<>();

        ObjectMapper om = new ObjectMapper();
        om.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        for (Column column : selectedColumns) {
            String attributeName = column.getExternalColumnId();
            Object attributeValue = null;

            if (attributes.containsKey(attributeName)) {
                attributeValue = attributes.get(attributeName);
            }
            attributeValues.add(attributeValue);
        }

        return attributeValues;
    }

    private void doNameLocationBasedLookup(MatchInput input, MatchContext matchContext,
            List<Triple<InternalOutputRecord, AccountLookupRequest, MatchContext>> lookupRequestTriplets) {
        // TODO - need to be implemented by M7

        if (input.getReturnUnmatched()) {
            for (Triple<InternalOutputRecord, AccountLookupRequest, MatchContext> triplet : lookupRequestTriplets) {
                if (triplet.getMiddle() == null) {
                    InternalOutputRecord record = triplet.getLeft();
                    record.setOutput(record.getInput());
                    record.setMatched(false);
                }
            }

        } else {
            throw new NotImplementedException("Impl of name location based lookup is yet to be implemented");
        }
    }
}
