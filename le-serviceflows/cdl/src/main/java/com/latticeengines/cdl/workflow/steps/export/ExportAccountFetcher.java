package com.latticeengines.cdl.workflow.steps.export;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
                        ? "null"
                        : JsonUtils.serialize(segmentExportContext.getClonedAccountFrontEndQuery())));
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
            log.info("Failed to match adequately, back to old logic for extracting account data from Redshift");
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
        List<List<Object>> data = new ArrayList<>();
        internalAccountIds.forEach(accountId -> data.add(Collections.singletonList(accountId)));
        List<String> lookupField = Collections.singletonList(InterfaceName.AccountId.name());
        Map<MatchKey, List<String>> kepMap = new HashMap<>();
        kepMap.put(MatchKey.LookupId, lookupField);

        Tenant tenant = new Tenant(customerSpace);
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(tenant);
        matchInput.setFields(lookupField);
        matchInput.setData(data);
        matchInput.setKeyMap(new HashMap<>());
        ColumnSelection customFieldSelection = new ColumnSelection();
        customFieldSelection.setColumns(fields);
        matchInput.setCustomSelection(customFieldSelection);
        String dataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        matchInput.setUseRemoteDnB(false);
        matchInput.setDataCloudVersion(dataCloudVersion);

        log.info(String.format("Calling matchapi with request payload: %s", JsonUtils.serialize(matchInput)));
        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);
        long matched = matchOutput.getResult().stream().filter(OutputRecord::isMatched).count();
        log.info(String.format("Match Response: %s requested, %s matched. %s unmatched", internalAccountIds.size(),
                matched, internalAccountIds.size() - matched));
        return convertToDataPage(matchOutput);
    }

    private DataPage convertToDataPage(MatchOutput matchOutput) {
        DataPage dataPage = new DataPage();
        List<Map<String, Object>> dataList = new ArrayList<>();
        dataPage.setData(dataList);

        Map<String, Object> data = null;
        if (matchOutput != null //
                && CollectionUtils.isNotEmpty(matchOutput.getResult()) //
                && matchOutput.getResult().get(0) != null) {

            if (matchOutput.getResult().get(0).isMatched() != Boolean.TRUE) {
                log.info("Didn't find any match from lattice data cloud. "
                        + "Still continue to process the result as we may "
                        + "have found partial match in my data table.");
            } else {
                log.info("Found full match from lattice data cloud as well as from my data table.");
            }

            final Map<String, Object> tempDataRef = new HashMap<>();
            List<String> fields = matchOutput.getOutputFields();
            List<Object> values = matchOutput.getResult().get(0).getOutput();
            IntStream.range(0, fields.size()) //
                    .forEach(i -> tempDataRef.put(fields.get(i), values.get(i)));
            data = tempDataRef;
        }

        if (MapUtils.isNotEmpty(data)) {
            dataPage.getData().add(data);
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
