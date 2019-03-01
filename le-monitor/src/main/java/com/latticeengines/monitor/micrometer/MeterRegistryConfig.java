package com.latticeengines.monitor.micrometer;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
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
import io.micrometer.core.instrument.config.MeterFilter;

/**
 * Root config for micrometer registry
 */
@Configuration
public class MeterRegistryConfig {

    private static final String TAG_TENANT = "Tenant";
    private static final String DEPLOYMENT_TEST_TENANT_NAME = "DeploymentTestNG";
    private static final String E2E_TEST_TENANT_NAME = "LETest";
    private static final List<String> TEST_TENANT_NAMES = Arrays.asList(DEPLOYMENT_TEST_TENANT_NAME,
            E2E_TEST_TENANT_NAME);

    private static final Logger log = LoggerFactory.getLogger(MeterRegistryConfig.class);

    @Value("${monitor.metrics.micrometer.enabled:false}")
    private boolean enableMonitoring;

    @Lazy
    @Bean(name = "rootRegistry")
    @Primary
    public MeterRegistry rootRegistry(@Lazy @Qualifier("influxMeterRegistry") MeterRegistry influxRegistry) {
        log.info("Initializing root meter registry, micrometer monitoring enabled = {}", enableMonitoring);
        return getCompositeRegistry(influxRegistry);
    }

    @Lazy
    @Bean(name = "rootHostRegistry")
    public MeterRegistry rootHostRegistry(
            @Lazy @Qualifier("influxHostMeterRegistry") MeterRegistry influxHostRegistry) {
        log.info("Initializing root host meter registry, micrometer monitoring enabled = {}", enableMonitoring);
        return getCompositeRegistry(influxHostRegistry);
    }

    private MeterRegistry getCompositeRegistry(MeterRegistry... registries) {
        CompositeMeterRegistry compositeRegistry = new CompositeMeterRegistry();
        if (!enableMonitoring || registries == null) {
            // return a noop registry
            return compositeRegistry;
        }

        Arrays.stream(registries).filter(Objects::nonNull).forEach(compositeRegistry::add);
        return addCommonFilters(compositeRegistry);
    }

    @Lazy
    @Bean
    public TimedAspect timedAspect(@Lazy @Qualifier("rootRegistry") MeterRegistry registry) {
        return new TimedAspect(registry);
    }

    private MeterRegistry addCommonFilters(MeterRegistry registry) {
        if (registry == null) {
            return null;
        }

        // not recording metrics for tests
        registry.config().meterFilter(excludeTestTenantFilter());
        return registry;
    }

    private MeterFilter excludeTestTenantFilter() {
        return MeterFilter.deny(id -> {
            String tenant = id.getTag(TAG_TENANT);
            if (StringUtils.isBlank(tenant)) {
                return false;
            }

            // tenant containing specific string is considered a test tenant
            return TEST_TENANT_NAMES.stream().anyMatch(tenant::contains);
        });
    }
}
