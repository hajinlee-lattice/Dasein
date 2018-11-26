package com.latticeengines.apps.lp.qbean;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.yarn.exposed.service.EMREnvService;

class EMRScalingCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(EMRScalingCallable.class);
    private static final ExecutorService pool = ThreadPoolUtils.getFixedSizeThreadPool("emr-scaling", 8);

    private List<String> scalingClusters;

    private EMRService emrService;

    private EMRCacheService emrCacheService;

    private EMREnvService emrEnvService;

    private void setScalingClusters(List<String> scalingClusters) {
        this.scalingClusters = scalingClusters;
    }

    private void setEmrService(EMRService emrService) {
        this.emrService = emrService;
    }

    public void setEmrCacheService(EMRCacheService emrCacheService) {
        this.emrCacheService = emrCacheService;
    }

    private void setEmrEnvService(EMREnvService emrEnvService) {
        this.emrEnvService = emrEnvService;
    }

    @Override
    public Boolean call() {
        try {
            if (CollectionUtils.isNotEmpty(scalingClusters)) {
                log.info("Invoking EMRScalingCallable. Scaling clusters: " + StringUtils.join(scalingClusters));
                List<Runnable> runnables = scalingClusters.stream() //
                        .filter(emrCluster -> StringUtils.isNotBlank(emrCacheService.getClusterId(emrCluster))) //
                        .map(emrCluster -> new EMRScalingRunnable(emrCluster, emrService, emrCacheService, emrEnvService)) //
                        .collect(Collectors.toList());
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

    static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private List<String> scalingClusters;

        private EMRService emrService;

        private EMRCacheService emrCacheService;

        private EMREnvService emrEnvService;

        Builder scalingClusters(String scalingClusters) {
            if (StringUtils.isNotBlank(scalingClusters)) {
                this.scalingClusters = Arrays.asList(scalingClusters.split(","));
            }
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
            callable.setEmrService(emrService);
            callable.setEmrCacheService(emrCacheService);
            callable.setEmrEnvService(emrEnvService);
            return callable;
        }

    }

}
