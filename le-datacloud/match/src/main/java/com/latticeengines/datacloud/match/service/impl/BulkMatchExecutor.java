package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.BeanDispatcher;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.MatchExecutor;

@Component("bulkMatchExecutor")
class BulkMatchExecutor extends MatchExecutorBase implements MatchExecutor {
    private static final Logger log = LoggerFactory.getLogger(BulkMatchExecutor.class);


    @Autowired
    private BeanDispatcher beanDispatcher;

    @Override
    public MatchContext execute(MatchContext matchContext) {
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        matchContext = dbHelper.fetchSync(matchContext);
        matchContext = complete(matchContext);
        return matchContext;
    }

    @Override
    public MatchContext executeAsync(MatchContext matchContext) {
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        return dbHelper.fetchAsync(matchContext);
    }

    @Override
    public MatchContext executeMatchResult(MatchContext matchContext) {
        log.error("$JAW$ In BulkMatchExecutor.executeMatchResult");

        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        dbHelper.fetchIdResult(matchContext);
        dbHelper.fetchMatchResult(matchContext);
        matchContext = complete(matchContext);
        return matchContext;
    }

    @Override
    public List<MatchContext> executeBulk(List<MatchContext> matchContexts) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

}
