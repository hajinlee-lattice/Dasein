package com.latticeengines.monitor.exposed.service;

import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Factory service for {@link MeterRegistry}
 */
public interface MeterRegistryFactoryService {

    /**
     * Return root {@link MeterRegistry} that contains all common tags for
     * microservices
     *
     * @return shared root registry
     */
    MeterRegistry getServiceLevelRegistry();

    /**
     * Return root {@link MeterRegistry} that contains all common tags for both
     * services and host machine
     *
     * @param metricDB the targeting InfuxDB Database
     * @return shared root registry
     */
    MeterRegistry getHostLevelRegistry(MetricDB metricDB);
}
