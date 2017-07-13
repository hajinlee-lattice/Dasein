package com.latticeengines.matchapi.service.impl;


import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datacloud.match.exposed.service.MatchCommandCleaner;

public class MatchCommandCleanerCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(MatchCommandCleanerCallable.class);

    private MatchCommandCleaner matchCommandCleaner;

    public MatchCommandCleanerCallable(Builder builder) {
        this.matchCommandCleaner = builder.getMatchCommandCleaner();
    }
    @Override
    public Boolean call() throws Exception {
        log.info("MatchCommandCleaner is triggered!");
        matchCommandCleaner.clean();
        return true;
    }

    public static class Builder {
        private MatchCommandCleaner matchCommandCleaner;
        public Builder() {
        }

        public MatchCommandCleaner getMatchCommandCleaner() {
            return this.matchCommandCleaner;
        }

        public Builder matchCommandCleaner(MatchCommandCleaner matchCommandCleaner) {
            this.matchCommandCleaner = matchCommandCleaner;
            return this;
        }


    }
}
