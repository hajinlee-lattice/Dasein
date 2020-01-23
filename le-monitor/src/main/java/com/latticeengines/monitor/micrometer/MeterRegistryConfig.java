package com.latticeengines.monitor.micrometer;

import static com.latticeengines.common.exposed.util.MetricUtils.TAG_ENVIRONMENT;
import static com.latticeengines.common.exposed.util.MetricUtils.TAG_SERVICE;
import static com.latticeengines.common.exposed.util.MetricUtils.TAG_STACK;
import static com.latticeengines.monitor.util.MonitoringUtils.getService;

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

import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.monitor.util.MonitoringUtils;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;

/**
 * Root config for micrometer registry
 */
@Configuration
public class MeterRegistryConfig {

    private static final String TAG_EMR_CLUSTER_NAME = "EmrClusterName";
    private static final String TAG_TENANT = "Tenant";
    private static final String DEPLOYMENT_TEST_TENANT_NAME = "DeploymentTestNG";
    private static final String E2E_TEST_TENANT_NAME = "LETest";
    private static final List<String> TEST_TENANT_NAMES = Arrays.asList(DEPLOYMENT_TEST_TENANT_NAME,
            E2E_TEST_TENANT_NAME);

    private static final Logger log = LoggerFactory.getLogger(MeterRegistryConfig.class);

    @Value("${monitor.metrics.micrometer.enabled}")
    private boolean enableMonitoring;

    @Value("${aws.emr.cluster:}")
    private String emrClusterName;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private String environment = MonitoringUtils.getEnvironment();

    private String stack = MonitoringUtils.getStack();

    private String hostname = MonitoringUtils.getHostName();

    @Lazy
    @Bean(name = "rootRegistry")
    @Primary
    public MeterRegistry rootRegistry(@Qualifier("influxMeterRegistry") MeterRegistry influxRegistry) {
        log.info("Initializing root meter registry, micrometer monitoring enabled = {}", enableMonitoring);
        return getCompositeRegistry(false, influxRegistry);
    }

    @Lazy
    @Bean(name = "rootHostRegistry")
    public MeterRegistry rootHostRegistry(
            @Qualifier("influxHostMeterRegistry") MeterRegistry influxHostRegistry) {
        log.info("Initializing root host meter registry, micrometer monitoring enabled = {}", enableMonitoring);
        return getCompositeRegistry(true, influxHostRegistry);
    }

    @Lazy
    @Bean(name = "rootInspectionHostRegistry")
    public MeterRegistry rootInspectionHostRegistry(
            @Qualifier("influxInspectionHostMeterRegistry") MeterRegistry influxInspectionHostMeterRegistry) {
        log.info("Initializing root Inspection host meter registry, micrometer monitoring enabled = {}", enableMonitoring);
        return getCompositeRegistry(true, influxInspectionHostMeterRegistry);
    }

    private MeterRegistry getCompositeRegistry(boolean addHostLevelTags, MeterRegistry... registries) {
        CompositeMeterRegistry compositeRegistry = new CompositeMeterRegistry();
        if (!enableMonitoring || registries == null) {
            // return a noop registry
            return compositeRegistry;
        }

        Arrays.stream(registries).filter(Objects::nonNull).forEach(compositeRegistry::add);
        return addCommonTags(addCommonFilters(compositeRegistry), addHostLevelTags);
    }

    @Lazy
    @Bean
    public TimedAspect timedAspect(@Lazy @Qualifier("rootRegistry") MeterRegistry registry) {
        return new TimedAspect(registry);
    }

    private MeterRegistry addCommonTags(MeterRegistry registry, boolean addHostLevelTags) {
        if (registry == null) {
            return null;
        }

        MeterRegistry.Config config = registry.config();
        // emr tag
        if (Boolean.TRUE.equals(useEmr) && MonitoringUtils.emrTagEnabled()) {
            config.commonTags(TAG_EMR_CLUSTER_NAME, emrClusterName);
        }

        // service level tags
        config.commonTags(TAG_ENVIRONMENT, environment, TAG_STACK, stack, TAG_SERVICE, getService());

        // host level tags
        if (addHostLevelTags) {
            config.commonTags(MetricUtils.TAG_HOST, hostname);
        }

        return registry;
    }

    private MeterRegistry addCommonFilters(MeterRegistry registry) {
        if (registry == null) {
            return null;
        }

        // not recording metrics for tests
        registry.config() //
                .meterFilter(excludeTestTenantFilter()) //
                .meterFilter(excludeSystemMetricsFilter());
        return registry;
    }

    // exclude all metrics by spring actuator for now, enable later if necessary
    private MeterFilter excludeSystemMetricsFilter() {
        return new MeterFilter() {
            @Override
            public MeterFilterReply accept(Meter.Id id) {
                if (id.getName().startsWith("tomcat.")) {
                    return MeterFilterReply.DENY;
                }
                if (id.getName().startsWith("jvm.")) {
                    return MeterFilterReply.DENY;
                }
                if (id.getName().startsWith("process.")) {
                    return MeterFilterReply.DENY;
                }
                if (id.getName().startsWith("system.")) {
                    return MeterFilterReply.DENY;
                }
                return MeterFilterReply.NEUTRAL;
            }
        };
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
