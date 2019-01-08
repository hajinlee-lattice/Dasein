package com.latticeengines.apps.lp.qbean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.yarn.exposed.service.EMREnvService;

class EMRScalingCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(EMRScalingCallable.class);
    private static final ExecutorService pool = ThreadPoolUtils.getFixedSizeThreadPool("emr-scaling", 8);

    private List<String> scalingClusters;

    private int minTaskNodes;

    private EMRService emrService;

    private EMRCacheService emrCacheService;

    private EMREnvService emrEnvService;

    private LoadingCache<String, List<ClusterSummary>> clusterSummaryCache;

    private void setScalingClusters(List<String> scalingClusters) {
        this.scalingClusters = scalingClusters;
    }

    public void setMinTaskNodes(int minTaskNodes) {
        this.minTaskNodes = minTaskNodes;
    }

    private void setEmrService(EMRService emrService) {
        this.emrService = emrService;
    }

    private void setEmrCacheService(EMRCacheService emrCacheService) {
        this.emrCacheService = emrCacheService;
    }

    private void setEmrEnvService(EMREnvService emrEnvService) {
        this.emrEnvService = emrEnvService;
    }

    @Override
    public Boolean call() {
        initClusterIdCache();
        try {
            if (CollectionUtils.isNotEmpty(scalingClusters)) {
                List<Runnable> runnables = new ArrayList<>();
                for (String pattern: scalingClusters) {
                    List<ClusterSummary> clusterSummaries = clusterSummaryCache.get(pattern);
                    List<String> names = new ArrayList<>();
                    if (CollectionUtils.isNotEmpty(clusterSummaries)) {
                        clusterSummaries.forEach(summary -> {
                            String clusterId = summary.getId();
                            String clusterName = summary.getName();
                            names.add(clusterName);
                            runnables.add(new EMRScalingRunnable(clusterName, clusterId, //
                                    minTaskNodes, emrService, emrCacheService, emrEnvService));
                        });
                    }
                    log.info(String.format("Found %d clusters by pattern [%s]: %s", //
                            CollectionUtils.size(names), pattern, names));
                }
                if (CollectionUtils.size(runnables) == 1) {
                    runnables.get(0).run();
                } else {
                    ThreadPoolUtils.runRunnablesInParallel(pool, runnables, 10, 1);
                }
            }
        } catch (Exception e) {
            log.error("Failed to run emr scaling job.", e);
        }
        return true;
    }

    private void initClusterIdCache() {
        if (clusterSummaryCache == null) {
            synchronized (this) {
                if (clusterSummaryCache == null) {
                    clusterSummaryCache = Caffeine.newBuilder() //
                            .maximumSize(100) //
                            .expireAfterWrite(10, TimeUnit.MINUTES) //
                            .build(this::loadClusterSummaries);
                }
            }
        }
    }

    private List<ClusterSummary> loadClusterSummaries(String pattern) {
        Pattern regex = Pattern.compile(pattern);
        return emrService.findClusters(clusterSummary -> {
            String name = clusterSummary.getName();
            return regex.matcher(name).matches();
        });
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private List<String> scalingClusters;

        private int minTaskNodes;

        private EMRService emrService;

        private EMRCacheService emrCacheService;

        private EMREnvService emrEnvService;

        Builder scalingClusters(String scalingClusters) {
            if (StringUtils.isNotBlank(scalingClusters)) {
                this.scalingClusters = Arrays.asList(scalingClusters.split(","));
            }
            return this;
        }

        Builder minTaskNodes(int minTaskNodes) {
            this.minTaskNodes = minTaskNodes;
            return this;
        }

        Builder emrService(EMRService emrService) {
            this.emrService = emrService;
            return this;
        }

        Builder emrCacheService(EMRCacheService emrCacheService) {
            this.emrCacheService = emrCacheService;
            return this;
        }

        Builder emrEnvService(EMREnvService emrEnvService) {
            this.emrEnvService = emrEnvService;
            return this;
        }

        EMRScalingCallable build() {
            EMRScalingCallable callable = new EMRScalingCallable();
            callable.setScalingClusters(scalingClusters);
            callable.setMinTaskNodes(minTaskNodes);
            callable.setEmrService(emrService);
            callable.setEmrCacheService(emrCacheService);
            callable.setEmrEnvService(emrEnvService);
            return callable;
        }

    }

}
