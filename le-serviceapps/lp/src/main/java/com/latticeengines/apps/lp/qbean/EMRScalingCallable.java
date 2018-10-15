package com.latticeengines.apps.lp.qbean;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EMRScalingCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(EMRScalingCallable.class);

    private List<String> scalingClusters;

    private void setScalingClusters(List<String> scalingClusters) {
        this.scalingClusters = scalingClusters;
    }

    @Override
    public Boolean call() {
        if (CollectionUtils.isNotEmpty(scalingClusters)) {
            log.info("Invoking EMRScalingCallable. Scaling clusters: " + StringUtils.join(scalingClusters));
        }
        return true;
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private List<String> scalingClusters;

        Builder scalingClusters(String scalingClusters) {
            if (StringUtils.isNotBlank(scalingClusters)) {
                this.scalingClusters = Arrays.asList(scalingClusters.split(","));
            }
            return this;
        }

        EMRScalingCallable build() {
            EMRScalingCallable callable = new EMRScalingCallable();
            callable.setScalingClusters(scalingClusters);
            return callable;
        }

    }

}
