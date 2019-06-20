package com.latticeengines.monitor.exposed.service;

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
     * @return shared root registry
     */
    MeterRegistry getHostLevelRegistry();
}
