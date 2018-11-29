package com.latticeengines.cdl.workflow.steps.play;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component
public class AccountFetcher {
    private static final Logger log = LoggerFactory.getLogger(AccountFetcher.class);

    @Autowired
    private EntityProxy entityProxy;

    // NOTE - do not increase this pagesize beyond 2.5K as it causes failure in
    // count query for corresponding counts. Also do not increase it beyond 250
    // otherwise contact fetch time increases significantly. After lot of trial
    // and error we found 150 to be a good number
    @Value("${playmaker.workflow.segment.pagesize:150}")
    private long pageSize;

    public long getCount(PlayLaunchContext playLaunchContext, DataCollection.Version version) {
        log.info(String.format("Requesting count for payload: %s", //
                playLaunchContext.getAccountFrontEndQuery() == null //
                        ? "null" : JsonUtils.serialize(playLaunchContext.getClonedAccountFrontEndQuery())));
        return entityProxy.getCountFromObjectApi( //
                playLaunchContext.getCustomerSpace().toString(), //
                playLaunchContext.getClonedAccountFrontEndQuery(), version);
    }

    public DataPage fetch(PlayLaunchContext playLaunchContext, //
            long segmentAccountsCount, long processedSegmentAccountsCount, DataCollection.Version version) {
        long expectedPageSize = Math.min(pageSize, (segmentAccountsCount - processedSegmentAccountsCount));
        FrontEndQuery accountFrontEndQuery = playLaunchContext.getClonedAccountFrontEndQuery();
        accountFrontEndQuery.setPageFilter(new PageFilter(processedSegmentAccountsCount, expectedPageSize));

        log.info(String.format("Account query => %s", JsonUtils.serialize(accountFrontEndQuery)));

        DataPage accountPage = entityProxy.getDataFromObjectApi(//
                playLaunchContext.getCustomerSpace().toString(), //
                accountFrontEndQuery, version);

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
