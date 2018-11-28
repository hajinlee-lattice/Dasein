package com.latticeengines.scoringapi.exposed.model.impl;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.primitives.Ints;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

@Component
public class ScoreArtifactCache {
    private static final Logger log = LoggerFactory.getLogger(ScoreArtifactCache.class);

    @Value("${scoringapi.scoreartifact.cache.max.weight}")
    private long scoreArtifactCacheMaxWeight;

    @Value("${scoringapi.scoreartifact.cache.ratio}")
    private int scoreArtifactCachePMMLFileRatio;

    @Value("${scoringapi.scoreartifact.cache.expiration.time}")
    private int scoreArtifactCacheExpirationTime;

    @Value("${scoringapi.scoreartifact.cache.concurrency.level}")
    private int scoreArtifactCacheConcurrencyLevel;

    @Value("${scoringapi.scoreartifact.cache.refresh.time:120}")
    private int scoreArtifactCacheRefreshTime;

    @Value("${scoringapi.scoreartifact.cache.default.size}")
    private long defaultPmmlFileSize;

    @Value("${scoringapi.scoreartifact.cache.max.threshold}")
    private double maxCacheThreshold;

    @Resource(name = "commonTaskScheduler")
    private ThreadPoolTaskScheduler taskScheduler;

    private ModelRetrieverImpl modelRetriever;

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts> scoreArtifactCache;

    void instantiateCache(ModelRetrieverImpl modelRetriever) {
        log.info(String.format("Instantiating score artifact cache with maxWeight=%d, and ratio=%d",
                scoreArtifactCacheMaxWeight, scoreArtifactCachePMMLFileRatio));
        scoreArtifactCache = CacheBuilder.newBuilder() //
                .recordStats() //
                .maximumWeight(scoreArtifactCacheMaxWeight) //
                .concurrencyLevel(scoreArtifactCacheConcurrencyLevel) //
                .expireAfterAccess(scoreArtifactCacheExpirationTime, TimeUnit.DAYS) //
                .weigher(new Weigher<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts>() {
                    @Override
                    public int weigh(AbstractMap.SimpleEntry<CustomerSpace, String> key, ScoringArtifacts value) {
                        return getWeightBasedOnPmmlFile(key, value);
                    }
                }) //
                .removalListener(
                        new RemovalListener<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts>() {
                            @Override
                            public void onRemoval(
                                    RemovalNotification<SimpleEntry<CustomerSpace, String>, ScoringArtifacts> removal) {
                                if (removal.wasEvicted()) {
                                    if (log.isInfoEnabled()) {
                                        log.info(String.format(
                                                "Removal of model artifacts for tenant=%s and model=%s. "//
                                                        + "due to cause=%s. Current cacheSize=%d",
                                                removal.getKey().getKey(), removal.getKey().getValue(),
                                                removal.getCause().name(), scoreArtifactCache.asMap().size()));
                                    }
                                }
                            }
                        }) //
                .build(new CacheLoader<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts>() {
                    @Override
                    public ScoringArtifacts load(AbstractMap.SimpleEntry<CustomerSpace, String> key) throws Exception {
                        if (log.isInfoEnabled()) {
                            log.info(String.format(
                                    "Load model artifacts for tenant=%s and model=%s. "//
                                            + "Current cacheSize=%d",
                                    key.getKey(), key.getValue(), scoreArtifactCache.asMap().size()));
                        }
                        long beforeLoad = System.currentTimeMillis();
                        ScoringArtifacts artifact = modelRetriever.retrieveModelArtifactsFromHdfs(key.getKey(),
                                key.getValue());
                        if (log.isInfoEnabled()) {
                            log.info(String.format(
                                    "Load completed model artifacts for tenant %s and model %s. "//
                                            + "Current cache size=%d. Duration=%d ms",
                                    key.getKey(), key.getValue(), scoreArtifactCache.asMap().size() + 1,
                                    System.currentTimeMillis() - beforeLoad));
                        }
                        return artifact;
                    };
                });
        this.modelRetriever = modelRetriever;
        boolean needToSchedule = Boolean.valueOf(System.getProperty("com.latticeengines.refreshScoreArtifactCache"));
        if (needToSchedule) {
            scheduleRefreshJob();
        }
    }

    public LoadingCache<SimpleEntry<CustomerSpace, String>, ScoringArtifacts> getCache() {
        return scoreArtifactCache;
    }

    void scheduleRefreshJob() {
        taskScheduler.scheduleWithFixedDelay(this::refreshCache,
                TimeUnit.SECONDS.toMillis(scoreArtifactCacheRefreshTime));
    }

    private void refreshCache() {
        log.info(String.format("Begin to refresh cache, the size of the cache=%d, with stats=%s",
                scoreArtifactCache.asMap().size(), scoreArtifactCache.stats().toString()));
        List<ModelSummary> modelSummaryListNeedsToRefresh = modelRetriever
                .getModelSummariesModifiedWithinTimeFrame(TimeUnit.SECONDS.toMillis(scoreArtifactCacheRefreshTime));
        if (CollectionUtils.isNotEmpty(modelSummaryListNeedsToRefresh)) {
            // get the modelsummary and its associated bucket metadata
            modelSummaryListNeedsToRefresh.forEach(modelsummay -> {
                CustomerSpace cs = CustomerSpace.parse(modelsummay.getTenant().getId());
                String modelId = modelsummay.getId();
                List<BucketMetadata> bucketMetadataList = modelRetriever.getBucketMetadata(cs, modelId);

                // lazy refresh by only updating the cache entry if present
                if (scoreArtifactCache.asMap().containsKey(modelRetriever.createModelKey(cs, modelId))) {
                    ScoringArtifacts scoringArtifacts = modelRetriever.getModelArtifacts(cs, modelId);
                    scoringArtifacts.setBucketMetadataList(bucketMetadataList);
                    scoringArtifacts.setModelSummary(modelsummay);
                    scoreArtifactCache.put(new AbstractMap.SimpleEntry<>(cs, modelId), scoringArtifacts);
                    log.info(
                            String.format("Refresh cache for model=%s in tenant=%s finishes.", modelId, cs.toString()));
                    log.info(String.format("After loading, the cacheSize= %d", scoreArtifactCache.asMap().size()));
                }
            });
        }
        log.info("Refresh cache ends");
        scoreArtifactCache.cleanUp();
    }

    @VisibleForTesting
    int getWeightBasedOnPmmlFile(AbstractMap.SimpleEntry<CustomerSpace, String> key, ScoringArtifacts value) {
        long pmmlFileSize = defaultPmmlFileSize;
        try {
            pmmlFileSize = modelRetriever.getSizeOfPMMLFile(key.getKey(), value.getModelSummary());
        } catch (Exception e) {
            log.warn(String.format("Error getting the pmml file size for modelId=%s. Setting it to default value=%d",
                    key.getValue(), defaultPmmlFileSize));
        }
        long weight = pmmlFileSize * scoreArtifactCachePMMLFileRatio;
        Map<String, Object> logMap = new TreeMap<>();
        logMap.put("model", key.getValue());
        logMap.put("weight", weight);
        log.info(JsonUtils.serialize(logMap));
        return Ints.checkedCast(throttleLargePmmlFileBasedOnWeight(weight, key.getValue()));
    }

    @VisibleForTesting
    long throttleLargePmmlFileBasedOnWeight(long weight, String modelId) {
        if (weight >= maxCacheThreshold * scoreArtifactCacheMaxWeight) {
            log.warn(String.format("The pmml file is too big to be loaded into the cache with modelId=%s, weight=%d",
                    modelId, weight));
            // throw new LedpException(LedpCode.LEDP_31026, new String[] {
            // modelId });
        }
        if (weight > Integer.MAX_VALUE) {
            log.warn(String.format(
                    "The pmml file with modelId=%s is greater than Integer.MAX_VALUE, force it to be MAX_VALUE",
                    modelId));
            return Integer.MAX_VALUE;
        }
        return weight;
    }

    @VisibleForTesting
    void setTaskScheduler(ThreadPoolTaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    @VisibleForTesting
    void setScoreArtifactCacheMaxWeight(long scoreArtifactCacheMaxWeight) {
        this.scoreArtifactCacheMaxWeight = scoreArtifactCacheMaxWeight;
    }

    @VisibleForTesting
    void setScoreArtifactCacheExpirationTime(int scoreArtifactCacheExpirationTime) {
        this.scoreArtifactCacheExpirationTime = scoreArtifactCacheExpirationTime;
    }

    @VisibleForTesting
    void setScoreArtifactCachePMMLFileRatio(int scoreArtifactCachePMMLFileRatio) {
        this.scoreArtifactCachePMMLFileRatio = scoreArtifactCachePMMLFileRatio;
    }

    @VisibleForTesting
    void setScoreArtifactCacheRefreshTime(int scoreArtifactCacheRefreshTime) {
        this.scoreArtifactCacheRefreshTime = scoreArtifactCacheRefreshTime;
    }

    @VisibleForTesting
    void setScoreArtifactCacheConcurrencyLevel(int concurrencyLevel) {
        this.scoreArtifactCacheConcurrencyLevel = concurrencyLevel;
    }

    @VisibleForTesting
    void setScoreArtifactCacheMaxCacheThreshold(double maxCacheThreshold) {
        this.maxCacheThreshold = maxCacheThreshold;
    }

    @VisibleForTesting
    void setScoreArtifactCacheDefaultPmmlFileSize(long defaultPmmlFileSize) {
        this.defaultPmmlFileSize = defaultPmmlFileSize;
    }
}
