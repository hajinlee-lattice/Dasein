package com.latticeengines.monitor.micrometer;

import java.time.Duration;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;
import com.latticeengines.monitor.util.MonitoringUtils;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;

/**
 * Default config for influx micrometer registry
 */
@Configuration
public class InfluxRegistryConfig {

    private Logger log = LoggerFactory.getLogger(InfluxRegistryConfig.class);

    @Value("${monitor.metrics.micrometer.influxdb.enabled:false}")
    private boolean enableMonitoring;

    @Value("${monitor.influxdb.url}")
    private String url;

    @Value("${monitor.influxdb.username}")
    private String username;

    @Value("${monitor.influxdb.password}")
    private String password;

    @Value("${monitor.metrics.micrometer.influxdb.step.minute:1}")
    private long stepInMinutes;

    @Value("${monitor.metrics.micrometer.influxdb.num.threads:2}")
    private int numThreads;

    @Value("${monitor.metrics.micrometer.influxdb.autocreatedb:false}")
    private boolean autoCreateDb;

    @Value("${monitor.influxdb.environment:Local}")
    private String environment;

    @Value("${monitor.influxdb.stack:unknown}")
    private String stack;

    private String hostname = MonitoringUtils.getHostName();

    // use one month retention policy for now
    private RetentionPolicy policy = RetentionPolicyImpl.ONE_MONTH;

    @Lazy
    @Bean(name = "influxMeterRegistry")
    public MeterRegistry influxRegistry() {
        InfluxConfig config = getInfluxConfig(MetricDB.LDC_Match.getDbName());
        log.info("Instantiating InfluxMeterRegistry... url={},db={},enabled={},step={}m", config.uri(), config.db(),
                config.enabled(), config.step());
        return getInfluxRegistry(config);
    }

    @Lazy
    @Bean(name = "influxHostMeterRegistry")
    public MeterRegistry influxHostRegistry() {
        InfluxConfig config = getInfluxConfig(MetricDB.LDC_Match.getDbName());
        MeterRegistry registry = getInfluxRegistry(config);
        log.info("Instantiating InfluxHostMeterRegistry... url={},db={},enabled={},step={}m", config.uri(), config.db(),
                config.enabled(), config.step());
        // set hostname tags
        registry.config().commonTags(MetricUtils.TAG_HOST, hostname);
        return registry;
    }

    /*
     * helper to set env & stack common tags
     */
    private MeterRegistry getInfluxRegistry(@NotNull InfluxConfig config) {
        MeterRegistry registry = new InfluxMeterRegistry(config, Clock.SYSTEM);
        // set common tags
        registry.config().commonTags(MetricUtils.TAG_ENVIRONMENT, environment, MetricUtils.TAG_STACK, stack);
        return registry;
    }

    private InfluxConfig getInfluxConfig(@NotNull String db) {
        return new InfluxConfig() {

            @Override
            public String uri() {
                return url;
            }

            @Override
            public String db() {
                return db;
            }

            @Override
            public boolean autoCreateDb() {
                return autoCreateDb;
            }

            @Override
            public String userName() {
                return StringUtils.isNotBlank(username) ? username : null;
            }

            @Override
            public String password() {
                return StringUtils.isNotBlank(password) ? password : null;
            }

            @Override
            public boolean enabled() {
                return enableMonitoring;
            }

            @Override
            public Duration step() {
                return Duration.ofMinutes(stepInMinutes);
            }

            @Override
            public int numThreads() {
                return numThreads;
            }

            @Override
            public String retentionPolicy() {
                return policy.getName();
            }

            @Override
            public String retentionDuration() {
                return policy.getDuration();
            }

            @Override
            public Integer retentionReplicationFactor() {
                return policy.getReplication();
            }

            @Override
            public String get(String key) {
                return null;
            }
        };
    }
}
