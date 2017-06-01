package com.latticeengines.scoringapi.exposed.model.impl;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

@Component
public class ScoreArtifactCache {
    private static final Log log = LogFactory.getLog(ScoreArtifactCache.class);

    @Value("${scoringapi.scoreartifact.cache.maxsize}")
    private int scoreArtifactCacheMaxSize;

    @Value("${scoringapi.scoreartifact.cache.expiration.time}")
    private int scoreArtifactCacheExpirationTime;

    @Value("${scoringapi.scoreartifact.cache.refresh.time:120}")
    private int scoreArtifactCacheRefreshTime;

    @Autowired
    @Qualifier("commonTaskScheduler")
    private ThreadPoolTaskScheduler taskScheduler;

    private ModelRetrieverImpl modelRetriever;

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts> scoreArtifactCache;

    void instantiateCache(ModelRetrieverImpl modelRetriever) {
        log.info("Instantiating score artifact cache with max size " + scoreArtifactCacheMaxSize);
        scoreArtifactCache = CacheBuilder.newBuilder().maximumSize(scoreArtifactCacheMaxSize) //
                .expireAfterAccess(scoreArtifactCacheExpirationTime, TimeUnit.DAYS) //
                .build(new CacheLoader<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts>() {
                    @Override
                    public ScoringArtifacts load(AbstractMap.SimpleEntry<CustomerSpace, String> key) throws Exception {
                        if (log.isInfoEnabled()) {
                            log.info(String.format("Load model artifacts for tenant %s and model %s", key.getKey(),
                                    key.getValue()));
                        }
                        ScoringArtifacts artifact = modelRetriever.retrieveModelArtifactsFromHdfs(key.getKey(),
                                key.getValue());
                        if (log.isInfoEnabled()) {
                            log.info(String.format("Load completed model artifacts for tenant %s and model %s",
                                    key.getKey(), key.getValue()));
                        }
                        return artifact;
                    };
                });
        this.modelRetriever = modelRetriever;
        scheduleRefreshJob();
    }

    public LoadingCache<SimpleEntry<CustomerSpace, String>, ScoringArtifacts> getCache() {
        return scoreArtifactCache;
    }

    void scheduleRefreshJob() {
        taskScheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                refreshCache();
            }
        }, TimeUnit.SECONDS.toMillis(scoreArtifactCacheRefreshTime));
    }

    private void refreshCache() {
        log.info("Begin to refresh cache");
        List<ModelSummary> modelSummaryListNeedsToRefresh = modelRetriever
                .getModelSummariesModifiedWithinTimeFrame(TimeUnit.SECONDS.toMillis(scoreArtifactCacheRefreshTime));
        if (CollectionUtils.isNotEmpty(modelSummaryListNeedsToRefresh)) {
            // get the modelsummary and its associated bucket metadata
            modelSummaryListNeedsToRefresh.forEach(modelsummay -> {
                CustomerSpace cs = CustomerSpace.parse(modelsummay.getTenant().getId());
                String modelId = modelsummay.getId();
                List<BucketMetadata> bucketMetadataList = modelRetriever.getBucketMetadata(cs, modelId);
                ScoringArtifacts scoringArtifacts = modelRetriever.getModelArtifactsIfPresent(cs, modelId);
                // lazy refresh by only updating the cache entry if present
                if (scoringArtifacts != null) {
                    scoringArtifacts.setBucketMetadataList(bucketMetadataList);
                    scoringArtifacts.setModelSummary(modelsummay);
                    scoreArtifactCache.put(new AbstractMap.SimpleEntry<CustomerSpace, String>(cs, modelId),
                            scoringArtifacts);
                    log.info(
                            String.format("Refresh cache for model %s in tenant %s finishes.", modelId, cs.toString()));
                }
            });
        }
        log.info("Refresh cache ends");
    }

    @VisibleForTesting
    void setTaskScheduler(ThreadPoolTaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    @VisibleForTesting
    void setScoreArtifactCacheMaxSize(int scoreArtifactCacheMaxSize) {
        this.scoreArtifactCacheMaxSize = scoreArtifactCacheMaxSize;
    }

    @VisibleForTesting
    void setScoreArtifactCacheExpirationTime(int scoreArtifactCacheExpirationTime) {
        this.scoreArtifactCacheExpirationTime = scoreArtifactCacheExpirationTime;
    }

    @VisibleForTesting
    void setScoreArtifactCacheRefreshTime(int scoreArtifactCacheRefreshTime) {
        this.scoreArtifactCacheRefreshTime = scoreArtifactCacheRefreshTime;
    }
}
