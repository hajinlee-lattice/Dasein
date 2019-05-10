package com.latticeengines.cdl.workflow.steps.export;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportAccountFetcher {
    private static final Logger log = LoggerFactory.getLogger(ExportAccountFetcher.class);

    @Autowired
    private EntityProxy entityProxy;

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Value("${playmaker.workflow.segment.pagesize:200}")
    private long pageSize;

    public long getCount(SegmentExportContext segmentExportContext, DataCollection.Version version) {
        log.info(String.format("Requesting count for payload: %s", //
                segmentExportContext.getAccountFrontEndQuery() == null //
                        ? "null" : JsonUtils.serialize(segmentExportContext.getClonedAccountFrontEndQuery())));
        return entityProxy.getCountFromObjectApi( //
                segmentExportContext.getCustomerSpace().toString(), //
                segmentExportContext.getClonedAccountFrontEndQuery(), version);
    }

    public DataPage fetch(SegmentExportContext segmentExportContext, //
            long segmentAccountsCount, long processedSegmentAccountsCount, DataCollection.Version version) {
        long expectedPageSize = Math.min(pageSize, (segmentAccountsCount - processedSegmentAccountsCount));
        FrontEndQuery accountFrontEndQuery = segmentExportContext.getClonedAccountFrontEndQuery();
        accountFrontEndQuery.setPageFilter(new PageFilter(processedSegmentAccountsCount, expectedPageSize));

        log.info(String.format("Account query => %s", JsonUtils.serialize(accountFrontEndQuery)));
        List<Lookup> originalLookups = accountFrontEndQuery.getLookups();

        accountFrontEndQuery.setLookups(originalLookups.stream().map(l -> (AttributeLookup) l)
                .filter(l -> l.getAttribute().equals(InterfaceName.AccountId.name())).collect(Collectors.toList()));
        DataPage accountPage = entityProxy.getDataFromObjectApi( //
                segmentExportContext.getCustomerSpace().toString(), //
                accountFrontEndQuery, version, true);

        List<Object> accountIds = accountPage.getData().stream().map(acc -> acc.get(InterfaceName.AccountId.name()))
                .collect(Collectors.toList());

        try {
            List<Column> matchFields = originalLookups.stream().map(l -> (AttributeLookup) l)
                    .map(attr -> new Column(attr.getAttribute())).collect(Collectors.toList());

            accountPage = getAccountByIdViaMatchApi(segmentExportContext.getCustomerSpace().toString(), accountIds,
                    matchFields);
        } catch (Exception e) {
            log.error("Failed to get data for accounts from matchApi", e);
            accountPage = null;
        }

        if (accountPage == null || CollectionUtils.isEmpty(accountPage.getData())
                || accountPage.getData().size() != accountIds.size()) {
            log.info("Failed to match adequately, reverting to old logic for extracting account data from Redshift");
            accountFrontEndQuery.setLookups(originalLookups);
            accountPage = entityProxy.getDataFromObjectApi( //
                    segmentExportContext.getCustomerSpace().toString(), //
                    accountFrontEndQuery, version, true);
        }

        log.info(String.format("Got # %d elements in this loop", accountPage.getData().size()));
        return accountPage;
    }

    private DataPage getAccountByIdViaMatchApi(String customerSpace, List<Object> internalAccountIds,
            List<Column> fields) {
        if (CollectionUtils.isEmpty(internalAccountIds)) {
            return null;
        }

        List<List<Object>> data = new ArrayList<>();
        internalAccountIds.forEach(accountId -> data.add(Collections.singletonList(accountId)));
        List<String> lookupField = Collections.singletonList(InterfaceName.AccountId.name());
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.LookupId, lookupField);

        Tenant tenant = new Tenant(customerSpace);
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(tenant);
        matchInput.setFields(lookupField);
        matchInput.setData(data);
        matchInput.setKeyMap(keyMap);
        ColumnSelection customFieldSelection = new ColumnSelection();
        customFieldSelection.setColumns(fields);
        matchInput.setCustomSelection(customFieldSelection);
        String dataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        matchInput.setUseRemoteDnB(false);
        matchInput.setDataCloudVersion(dataCloudVersion);
        matchInput.setOperationalMode(OperationalMode.CDL_LOOKUP);

        log.info(String.format("Calling matchapi with request payload: %s", JsonUtils.serialize(matchInput)));
        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);
        long matched = matchOutput.getResult().stream().filter(OutputRecord::isMatched).count();
        log.info(String.format("Match Response: %s requested, %s matched. %s unmatched", internalAccountIds.size(),
                matched, internalAccountIds.size() - matched));
        return convertToDataPage(matchOutput);
    }

    @VisibleForTesting
    DataPage convertToDataPage(MatchOutput matchOutput) {
        DataPage dataPage = new DataPage();
        if (matchOutput != null //
                && CollectionUtils.isNotEmpty(matchOutput.getResult())) {
            long unmatched = matchOutput.getResult().stream().filter(output -> !output.isMatched()).count();
            if (unmatched != 0) {
                log.info("Unable to fully match given accounts, " + unmatched + " accounts of "
                        + matchOutput.getResult().size() + "were not matched");
                return null;
            } else {
                log.info("Found full match from lattice data cloud as well as from my data table.");
            }

            List<Map<String, Object>> dataList = new ArrayList<>();
            dataPage.setData(dataList);
            final List<String> fields = matchOutput.getOutputFields();
            for (OutputRecord r : matchOutput.getResult()) {
                List<Object> values = r.getOutput();
                Map<String, Object> tempDataRef = new HashMap<>();
                IntStream.range(0, fields.size()) //
                        .forEach(i -> {
                            tempDataRef.put(fields.get(i), values.get(i));
                        });
                dataPage.getData().add(tempDataRef);
            }
        }
        return dataPage;
    }

    @VisibleForTesting
    void setEntityProxy(EntityProxy entityProxy) {
        this.entityProxy = entityProxy;
    }

    @VisibleForTesting
    void setMatchProxy(MatchProxy matchProxy) {
        this.matchProxy = matchProxy;
    }

    @VisibleForTesting
    void setColumnMetadataProxy(ColumnMetadataProxy columnMetadataProxy) {
        this.columnMetadataProxy = columnMetadataProxy;
    }

    @VisibleForTesting
    void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }
}
