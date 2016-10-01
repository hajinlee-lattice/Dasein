package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.DbHelper;
import com.latticeengines.datacloud.match.service.MatchFetcher;


@Component("bulkMatchFetcher")
public class BulkMatchFetcher implements MatchFetcher {

    @Autowired
    private BeanDispatcherImpl beanDispatcher;

    @Override
    public MatchContext fetch(MatchContext matchContext) {
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        return dbHelper.fetch(matchContext);
    }

    @Override
    public List<String> enqueue(List<MatchContext> matchContexts) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

    @Override
    public List<MatchContext> waitForResult(List<String> rootUids) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

}
