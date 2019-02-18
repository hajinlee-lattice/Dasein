package com.latticeengines.monitor.micrometer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

/**
 * Root config for micrometer registry
 */
@Configuration
public class MeterRegistryConfig {

    private Logger log = LoggerFactory.getLogger(MeterRegistryConfig.class);

    @Value("${monitor.metrics.micrometer.enabled:false}")
    private boolean enableMonitoring;

    @Lazy
    @Bean(name = "rootRegistry")
    @Primary
    public MeterRegistry rootRegistry(@Lazy @Qualifier("influxMeterRegistry") MeterRegistry influxRegistry) {
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        log.info("Initializing root meter registry, micrometer monitoring enabled = {}", enableMonitoring);
        if (!enableMonitoring) {
            // return a noop registry
            return registry;
        }

        if (influxRegistry != null) {
            registry.add(influxRegistry);
        }
        return registry;
    }

    @Lazy
    @Bean
    public TimedAspect timedAspect(@Lazy @Qualifier("rootRegistry") MeterRegistry registry) {
        return new TimedAspect(registry);
    }
}
