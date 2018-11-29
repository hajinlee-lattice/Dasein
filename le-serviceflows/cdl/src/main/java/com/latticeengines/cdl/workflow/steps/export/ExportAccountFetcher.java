package com.latticeengines.cdl.workflow.steps.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportAccountFetcher {
    private static final Logger log = LoggerFactory.getLogger(ExportAccountFetcher.class);

    @Autowired
    private EntityProxy entityProxy;

    @Value("${playmaker.workflow.segment.pagesize:100}")
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
            long segmentAccountsCount, long processedSegmentAccountsCount,
            DataCollection.Version version) {
        long expectedPageSize = Math.min(pageSize, (segmentAccountsCount - processedSegmentAccountsCount));
        FrontEndQuery accountFrontEndQuery = segmentExportContext.getClonedAccountFrontEndQuery();
        accountFrontEndQuery.setPageFilter(new PageFilter(processedSegmentAccountsCount, expectedPageSize));

        log.info(String.format("Account query => %s", JsonUtils.serialize(accountFrontEndQuery)));

        DataPage accountPage = entityProxy.getDataFromObjectApi( //
                segmentExportContext.getCustomerSpace().toString(), //
                accountFrontEndQuery, version, true);

        log.info(String.format("Got # %d elements in this loop", accountPage.getData().size()));
        return accountPage;
    }

    @VisibleForTesting
    void setEntityProxy(EntityProxy entityProxy) {
        this.entityProxy = entityProxy;
    }

    @VisibleForTesting
    void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }
}
