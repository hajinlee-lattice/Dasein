package com.latticeengines.monitor.exposed.metrics.impl;

import static com.latticeengines.common.exposed.metric.MetricNames.Invocation.METRIC_INVOCATION_ERROR;
import static com.latticeengines.common.exposed.metric.MetricNames.Invocation.METRIC_INVOCATION_GLBOAL_HISTORY;
import static com.latticeengines.common.exposed.metric.MetricNames.Invocation.METRIC_INVOCATION_HISTORY;
import static com.latticeengines.common.exposed.metric.MetricTags.TAG_TENANT;
import static com.latticeengines.common.exposed.metric.MetricTags.Invocation.TAG_CAN_IGNORE;
import static com.latticeengines.common.exposed.metric.MetricTags.Invocation.TAG_GENERIC;
import static com.latticeengines.common.exposed.metric.MetricTags.Invocation.TAG_HAS_ERROR;
import static com.latticeengines.common.exposed.metric.MetricTags.Invocation.TAG_METHOD_NAME;

import java.lang.reflect.Method;
import java.util.List;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;
import com.latticeengines.monitor.exposed.annotation.InvocationMeter;
import com.latticeengines.monitor.exposed.annotation.IgnoreGlobalApiMeter;
import com.latticeengines.monitor.exposed.metrics.InvocationMeterAspect;
import com.latticeengines.monitor.exposed.service.MeterRegistryFactoryService;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;

@Aspect
public class InvocationMeterAspectImpl implements InvocationMeterAspect {

    private static final Logger log = LoggerFactory.getLogger(InvocationMeterAspectImpl.class);

    @Lazy
    @Inject
    private MeterRegistryFactoryService registryFactory;

    @Around("@annotation(com.latticeengines.monitor.exposed.annotation.InvocationMeter) " + //
            "|| @annotation(com.latticeengines.monitor.exposed.annotation.InvocationMeters)")
    public Object incrementInvocationMeter(ProceedingJoinPoint joinPoint) throws Throwable {
        Throwable t = null;
        Object toReturn = null;
        long start = System.currentTimeMillis();
        try {
            toReturn = joinPoint.proceed();
        } catch (Throwable e) {
            t = e;
        }
        long duration = System.currentTimeMillis() - start;

        // do not switch thread, some instrument needs the MultiTenantContext
        // if have to switch thread, do that after getting tenantId
        try {
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            Object[] args = joinPoint.getArgs();
            incrementInvocationCounter(signature, args, toReturn, t, duration);
            incrementErrorCounter(signature, args, toReturn, t);
        } catch (Exception e) {
            log.warn("Failed to log the invocation metrics", e);
        }

        if (t == null) {
            return toReturn;
        } else {
            throw t;
        }
    }

    @Around("@annotation(io.swagger.annotations.ApiOperation)")
    public Object globalInvocationMeter(ProceedingJoinPoint joinPoint) throws Throwable {
        Throwable t = null;
        Object toReturn = null;
        long start = System.currentTimeMillis();
        try {
            toReturn = joinPoint.proceed();
        } catch (Throwable e) {
            t = e;
        }
        long duration = System.currentTimeMillis() - start;

        // do not switch thread, some instrument needs the MultiTenantContext
        // if have to switch thread, do that after getting tenantId
        try {
            String fullName = joinPoint.getTarget().getClass().getCanonicalName();
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            fullName += signature.getName();
            boolean canIgnore = signature.getMethod().getAnnotation(IgnoreGlobalApiMeter.class) != null;
            incrementGlobalInvocationMeter(fullName, duration, t != null, canIgnore);
        } catch (Exception e) {
            log.warn("Failed to log the invocation metrics", e);
        }

        if (t == null) {
            return toReturn;
        } else {
            throw t;
        }
    }

    private void incrementGlobalInvocationMeter(String name, long duration, boolean hasError, boolean canIgnore) {
        MeterRegistry meterRegistry =  registryFactory.getGlobalHourlyRegistry();
        DistributionSummary.Builder builder = DistributionSummary //
                .builder(METRIC_INVOCATION_GLBOAL_HISTORY) //
                .tag(TAG_METHOD_NAME, name) //
                .tag(TAG_CAN_IGNORE, String.valueOf(canIgnore)) //
                .tag(TAG_HAS_ERROR, String.valueOf(hasError));
        builder.register(meterRegistry).record(duration);
    }

    private void incrementInvocationCounter(MethodSignature signature, Object[] args, Object toReturn, Throwable t,
                                            long duration) {
        InvocationMeter[] meters = getInvocationMeters(signature);
        MeterRegistry meterRegistry =  registryFactory.getHostLevelRegistry(MetricDB.INSPECTION);
        for (InvocationMeter meter: meters) {
            InvocationInstrument instrument = InstrumentRegistry.getInstrument(meter.instrument());
            if (instrument.accept(signature, args, toReturn, t)) {
                String meterName = meter.name();
                String measurement = meter.measurment();
                String tenantId = getTenantId(instrument, signature, args);
                double numReqs = instrument.getNumReqs(signature, args, toReturn, t);
                List<String> genericTags = instrument.getGenericTagValues(signature, args, toReturn, t);
                for (int i = 0; i < numReqs; i++) {
                    DistributionSummary.Builder builder = DistributionSummary //
                            .builder(appendMeasurementName(METRIC_INVOCATION_HISTORY, measurement)) //
                            .tag(TAG_METHOD_NAME, meterName) //
                            .tag(TAG_TENANT, tenantId);
                    addGenericTags(builder::tag, genericTags);
                    builder.register(meterRegistry).record(duration);
                }
            }
        }
    }

    private void incrementErrorCounter(MethodSignature signature, Object[] args, Object toReturn, Throwable t) {
        InvocationMeter[] meters = getInvocationMeters(signature);
        MeterRegistry meterRegistry =  registryFactory.getHostLevelRegistry(MetricDB.INSPECTION);
        for (InvocationMeter meter: meters) {
            if (meter.errors()) {
                InvocationInstrument instrument = InstrumentRegistry.getInstrument(meter.instrument());
                if (instrument.accept(signature, args, toReturn, t)) {
                    String meterName = meter.name();
                    String measurement = meter.measurment();
                    String tenantId = getTenantId(instrument, signature, args);
                    List<String> genericTags = instrument.getGenericTagValues(signature, args, toReturn, t);
                    double numErrors;
                    if (t != null) {
                        numErrors = instrument.getNumErrors(signature, args, t);
                    } else {
                        numErrors = instrument.getNumErrors(signature, args, toReturn);
                    }
                    Counter.Builder builder = Counter //
                            .builder(appendMeasurementName(METRIC_INVOCATION_ERROR, measurement)) //
                            .tag(TAG_METHOD_NAME, meterName) //
                            .tag(TAG_TENANT, tenantId);
                    addGenericTags(builder::tag, genericTags);
                    builder.register(meterRegistry).increment(numErrors);
                }
            }
        }
    }

    private String getTenantId(InvocationInstrument instrument, MethodSignature signature, Object[] args) {
        String tenantId = instrument.getTenantId(signature, args);
        if (StringUtils.isBlank(tenantId)) {
            tenantId = InvocationInstrument.UNKOWN_TAG_VALUE;
        }
        return tenantId;
    }

    private void addGenericTags(Tagger tagger, List<String> vals) {
        if (CollectionUtils.isNotEmpty(vals)) {
            for (int i = 0; i < vals.size(); i++) {
                String val = vals.get(i);
                String key = String.format(TAG_GENERIC, i);
                if (val != null) {
                    tagger.tag(key, val);
                }
            }
        }
    }

    private InvocationMeter[] getInvocationMeters(MethodSignature signature) {
        Method method = signature.getMethod();
        return method.getAnnotationsByType(InvocationMeter.class);
    }

    private static String appendMeasurementName(@NotNull String orig, String measurement) {
        return StringUtils.isNotBlank(measurement) ? orig + "_" + measurement : orig;
    }

    private interface Tagger {
        void tag(String key, String val);
    }

}

