package com.latticeengines.matchapi.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.MatchCommandCleaner;
import com.latticeengines.matchapi.service.impl.MatchCommandCleanerCallable;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("matchCommandCleanerBean")
public class MatchCommandCleanerBean implements QuartzJobBean {

    @Inject
    private MatchCommandCleaner matchCommandCleaner;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        MatchCommandCleanerCallable.Builder builder = new MatchCommandCleanerCallable.Builder();
        builder.matchCommandCleaner(matchCommandCleaner);
        return new MatchCommandCleanerCallable(builder);
    }
}
