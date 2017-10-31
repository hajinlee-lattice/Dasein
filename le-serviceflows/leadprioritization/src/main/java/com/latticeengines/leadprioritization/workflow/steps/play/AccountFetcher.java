package com.latticeengines.leadprioritization.workflow.steps.play;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component
public class AccountFetcher {
    private static final Logger log = LoggerFactory.getLogger(AccountFetcher.class);

    @Autowired
    private EntityProxy entityProxy;

    @Value("${playmaker.workflow.segment.pagesize:100}")
    private long pageSize;

    public long getCount(PlayLaunchContext playLaunchContext) {
        log.info(String.format("Requesting count for payload: %s", //
                playLaunchContext.getAccountFrontEndQuery() == null //
                        ? "null" : JsonUtils.serialize(playLaunchContext.getClonedAccountFrontEndQuery())));
        return entityProxy.getCount( //
                playLaunchContext.getCustomerSpace().toString(), //
                playLaunchContext.getClonedAccountFrontEndQuery());
    }

    public DataPage fetch(PlayLaunchContext playLaunchContext, //
            long segmentAccountsCount, long processedSegmentAccountsCount) {
        long expectedPageSize = Math.min(pageSize, (segmentAccountsCount - processedSegmentAccountsCount));
        FrontEndQuery accountFrontEndQuery = playLaunchContext.getClonedAccountFrontEndQuery();
        accountFrontEndQuery.setPageFilter(new PageFilter(processedSegmentAccountsCount, expectedPageSize));

        log.info(String.format("Account query => %s", JsonUtils.serialize(accountFrontEndQuery)));

        DataPage accountPage = entityProxy.getData( //
                playLaunchContext.getCustomerSpace().toString(), //
                accountFrontEndQuery);

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
