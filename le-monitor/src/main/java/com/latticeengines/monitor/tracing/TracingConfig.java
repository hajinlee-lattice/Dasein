package com.latticeengines.monitor.tracing;

import static com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment.AppMaster;
import static com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment.TestClient;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import io.opentracing.util.GlobalTracer;

@Configuration
public class TracingConfig {

    private static final Logger log = LoggerFactory.getLogger(TracingConfig.class);
    private static final String DEFAULT_SERVICE = "default";
    private static final String TEST_CLIENT_SERVICE = "test-client";
    private static final String YARN_CONTAINER_SERVICE = "yarn-container";

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${monitor.tracing.enabled}")
    private boolean tracingEnabled;

    /*
     * Tracer, need to wait for bean environment for current env and service name
     */
    @Bean("tracer")
    @DependsOn("beanEnvironment")
    public Tracer tracer() {
        if (!tracingEnabled) {
            log.info("Tracing not enabled on stack {}, creating noop tracer", leStack);
            return NoopTracerFactory.create();
        }
        /*-
         * sample 100% for now
         */
        SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv() //
                .withType("const") //
                .withParam(1);
        /*-
         * allow log
         */
        String service = getServiceName();
        ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv() //
                .withLogSpans(true);
        io.jaegertracing.Configuration config = new io.jaegertracing.Configuration(service) //
                .withSampler(samplerConfig) //
                .withReporter(reporterConfig);
        JaegerTracer tracer = config.getTracer();
        // register global
        GlobalTracer.registerIfAbsent(tracer);
        // TODO add jaeger connection info, sampling info
        log.info("Instantiating jaeger tracer. stack={}, beanEnv={}, serviceName={}", leStack,
                BeanFactoryEnvironment.getEnvironment(), service);
        return tracer;
    }

    /*
     * helper to get tracing service name. format is <stack>-<service>
     */
    private String getServiceName() {
        BeanFactoryEnvironment.Environment env = BeanFactoryEnvironment.getEnvironment();
        if (env == AppMaster) {
            return wrapStack(YARN_CONTAINER_SERVICE);
        } else if (env == TestClient) {
            return wrapStack(TEST_CLIENT_SERVICE);
        }
        String service = BeanFactoryEnvironment.getService();
        return wrapStack(StringUtils.isNotBlank(service) ? service : DEFAULT_SERVICE);
    }

    private String wrapStack(@NotNull String service) {
        return String.format("%s-%s", leStack, service);
    }
}
