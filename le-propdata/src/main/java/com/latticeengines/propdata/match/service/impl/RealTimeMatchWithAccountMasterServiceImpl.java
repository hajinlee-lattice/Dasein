package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.latticeengines.domain.exposed.propdata.manage.Column;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchInput;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.propdata.match.LatticeAccount;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.AccountLookupService;

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

        List<Pair<InternalOutputRecord, AccountLookupRequest>> lookupRequestPairs = new ArrayList<>();
        AccountLookupRequest request = createLookupRequest(matchContext, lookupRequestPairs);

        if (request.getIds().size() < lookupRequestPairs.size()) {
            // TODO anoop - in M6 - add the logic for external lookup based on
            // namelocation from DnB matcher
            doNameLocationBasedLookup();
        }

        // TODO - anoop - currently assuming happy path - handle other scenarios
        // in next txn
        List<LatticeAccount> matchedResults = accountLookupService.batchLookup(request);

        MatchOutput output = createMatchOutput(input, matchContext, receivedAt, rootOperationUID, matchedResults);

        return output;
    }

    @Override
    @MatchStep(threshold = 0L)
    public BulkMatchOutput matchBulk(BulkMatchInput input) {
        // TODO - need to be implemented by Anoop in next txn
        throw new NotImplementedException("Impl of account master based match is yet to be implemented");
    }

    private AccountLookupRequest createLookupRequest(MatchContext matchContext,
            List<Pair<InternalOutputRecord, AccountLookupRequest>> lookupRequestPairs) {
        String dataCloudVersion = matchContext.getInput().getDataCloudVersion();
        AccountLookupRequest accountLookupRequest = new AccountLookupRequest(dataCloudVersion);

        for (InternalOutputRecord record : matchContext.getInternalResults()) {
            // we can not do direct lookup against account master if duns is not
            // specified and no valid domain specified
            boolean shouldCallExternalMatch = (StringUtils.isEmpty(record.getParsedDomain()) || record.isPublicDomain())
                    && StringUtils.isEmpty(record.getParsedDuns());

            if (!shouldCallExternalMatch) {
                accountLookupRequest
                        .addLookupPair((StringUtils.isEmpty(record.getParsedDomain()) || record.isPublicDomain()) ? null
                                : record.getParsedDomain(), record.getParsedDuns());
            }

            Pair<InternalOutputRecord, AccountLookupRequest> accountLookupRequestPair = new MutablePair<>(record,
                    accountLookupRequest);
            lookupRequestPairs.add(accountLookupRequestPair);
        }

        return accountLookupRequest;
    }

    private MatchOutput createMatchOutput(MatchInput input, MatchContext matchContext, Date receivedAt,
            String rootOperationUID, List<LatticeAccount> matchedResults) {
        List<OutputRecord> outputRecords = new ArrayList<>();
        MatchOutput output = matchContext.getOutput();
        output.setResult(outputRecords);
        ColumnSelection columnSelection = matchContext.getColumnSelection();
        List<Column> selectedColumns = columnSelection.getColumns();

        for (LatticeAccount result : matchedResults) {

            OutputRecord outputRecord = new OutputRecord();
            outputRecord.setMatched(result != null);
            outputRecord.setOutput(
                    outputRecord.isMatched() ? getAttributeValues(selectedColumns, result.getAttributes()) : null);

            outputRecords.add(outputRecord);
        }

        return output;
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

    private void doNameLocationBasedLookup() {
        // TODO - need to be implemented by Anoop by M6
        throw new NotImplementedException("Impl of name location based lookup is yet to be implemented");
    }
}
