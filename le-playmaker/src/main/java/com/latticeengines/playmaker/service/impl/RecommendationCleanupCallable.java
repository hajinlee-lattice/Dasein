package com.latticeengines.playmaker.service.impl;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.playmaker.service.RecommendationCleanupService;

public class RecommendationCleanupCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(RecommendationCleanupCallable.class);

    private RecommendationCleanupService recommendationCleanupService;

    public RecommendationCleanupCallable(Builder builder) {
        this.recommendationCleanupService = builder.getRecommendationCleanupService();
    }

    @Override
    public Boolean call() throws Exception {
        recommendationCleanupService.cleanup();
        return true;
    }

    public static class Builder {
        private RecommendationCleanupService recommendationCleanupService;

        public Builder() {
        }

        public RecommendationCleanupService getRecommendationCleanupService() {
            return recommendationCleanupService;
        }

        public Builder recommendationCleanupService(RecommendationCleanupService recommendationCleanupService) {
            this.recommendationCleanupService = recommendationCleanupService;
            return this;
        }
    }

}
