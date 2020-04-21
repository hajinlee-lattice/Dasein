package com.latticeengines.ulysses.aspect;

import javax.inject.Inject;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.ulysses.monitoring.UlyssesAccMeasurement;
import com.latticeengines.ulysses.monitoring.UlyssesAccMetrics;
import com.latticeengines.ulysses.monitoring.UlyssesAccSegmentMeasurement;
import com.latticeengines.ulysses.monitoring.UlyssesAccSegmentMetrics;
import com.latticeengines.ulysses.monitoring.UlyssesAttributeMeasurement;
import com.latticeengines.ulysses.monitoring.UlyssesAttributeMetrics;
import com.latticeengines.ulysses.monitoring.UlyssesPurchaseHistMeasurement;
import com.latticeengines.ulysses.monitoring.UlyssesPurchaseHistMetrics;
import com.latticeengines.ulysses.monitoring.UlyssesRecMeasurement;
import com.latticeengines.ulysses.monitoring.UlyssesRecMetrics;
import com.latticeengines.ulysses.monitoring.UlyssesTalkingPointsMeasurement;
import com.latticeengines.ulysses.monitoring.UlyssesTalkingPointsMetrics;

@Aspect
public class UlyssesMetricPublishAspect {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(UlyssesMetricPublishAspect.class);

    @Inject
    protected MetricService metricService;

    @Around("execution(* com.latticeengines.app.exposed.controller.PrimaryAttributeResource.*(..))")
    public Object ulyssesAttributeMonitor(ProceedingJoinPoint joinPoint) throws Throwable {

        Object retval;
        long timestamp = System.currentTimeMillis();
        retval = joinPoint.proceed();
        long timeTaken = System.currentTimeMillis() - timestamp;

        UlyssesAttributeMetrics metrics = generateUlyssesAttributeMetrics((int) timeTaken);
        UlyssesAttributeMeasurement measurement = new UlyssesAttributeMeasurement(metrics);
        metricService.write(MetricDB.ULYSSES, measurement);

        return retval;

    }

    private UlyssesAttributeMetrics generateUlyssesAttributeMetrics(int timeTaken) {
        UlyssesAttributeMetrics metrics = new UlyssesAttributeMetrics();
        metrics.setGetAttributeDurationMS(timeTaken);
        return metrics;
    }

    @Around("execution(* com.latticeengines.app.exposed.controller.DataLakeAccountResource.getAccountById(..))"
            + "&& execution(* com.latticeengines.app.exposed.controller.DataLakeAccountResource.getAccountsById(..))"
            + "&& execution(* com.latticeengines.app.exposed.controller.DataLakeAccountResource.getAccountByIdInDanteFormat(..))"
            + "&& execution(* com.latticeengines.app.exposed.controller.DataLakeAccountResource.getAccountsAndTalkingpoints(..))"
            + "&& args(attributeGroup, ..)")
    public Object ulyssesAccMonitor(ProceedingJoinPoint joinPoint, Predefined attributeGroup) throws Throwable {

        Object retval;
        long timestamp = System.currentTimeMillis();
        retval = joinPoint.proceed();
        long timeTaken = System.currentTimeMillis() - timestamp;

        UlyssesAccMetrics metrics = generateUlyssesAccMetrics((int) timeTaken, attributeGroup);
        UlyssesAccMeasurement measurement = new UlyssesAccMeasurement(metrics);
        metricService.write(MetricDB.ULYSSES, measurement);

        return retval;

    }

    private UlyssesAccMetrics generateUlyssesAccMetrics(int timeTaken, Predefined attributeGroup) {
        UlyssesAccMetrics metrics = new UlyssesAccMetrics();
        metrics.setAttributeName(attributeGroup.getName());
        metrics.setGetAccountDurationMS(timeTaken);
        return metrics;
    }

    @Around("execution(* com.latticeengines.app.exposed.controller.DataLakeAccountResource.getAccountSegmentsInDanteFormat(..))")
    public Object ulyssesAccSegmentMonitor(ProceedingJoinPoint joinPoint) throws Throwable {

        Object retval;
        long timestamp = System.currentTimeMillis();
        retval = joinPoint.proceed();
        long timeTaken = System.currentTimeMillis() - timestamp;

        UlyssesAccSegmentMetrics metrics = generateUlyssesAccSegmentMetrics((int) timeTaken);
        UlyssesAccSegmentMeasurement measurement = new UlyssesAccSegmentMeasurement(metrics);
        metricService.write(MetricDB.ULYSSES, measurement);

        return retval;

    }

    private UlyssesAccSegmentMetrics generateUlyssesAccSegmentMetrics(int timeTaken) {
        UlyssesAccSegmentMetrics metrics = new UlyssesAccSegmentMetrics();
        metrics.setGetAccountSegmentDurationMS(timeTaken);
        return metrics;
    }

    @Around("execution(* com.latticeengines.ulysses.controller.PurchaseHistoryResource.*(..))")
    public Object ulyssesPurchaseHistMonitor(ProceedingJoinPoint joinPoint) throws Throwable {

        Object retval;
        long timestamp = System.currentTimeMillis();
        retval = joinPoint.proceed();
        long timeTaken = System.currentTimeMillis() - timestamp;

        UlyssesPurchaseHistMetrics metrics = generateUlyssesPurchaseHistMetrics((int) timeTaken);
        UlyssesPurchaseHistMeasurement measurement = new UlyssesPurchaseHistMeasurement(metrics);
        metricService.write(MetricDB.ULYSSES, measurement);

        return retval;

    }

    private UlyssesPurchaseHistMetrics generateUlyssesPurchaseHistMetrics(int timeTaken) {
        UlyssesPurchaseHistMetrics metrics = new UlyssesPurchaseHistMetrics();
        metrics.setGetPurchaseHistDurationMS(timeTaken);
        return metrics;
    }

    @Around("execution(public * com.latticeengines.ulysses.controller.RecommendationResource.*(..))")
    public Object ulyssesRecommendationMonitor(ProceedingJoinPoint joinPoint) throws Throwable {

        Object retval;
        long timestamp = System.currentTimeMillis();
        retval = joinPoint.proceed();
        long timeTaken = System.currentTimeMillis() - timestamp;

        UlyssesRecMetrics metrics = generateUlyssesRecMetrics((int) timeTaken);
        UlyssesRecMeasurement measurement = new UlyssesRecMeasurement(metrics);
        metricService.write(MetricDB.ULYSSES, measurement);

        return retval;

    }

    private UlyssesRecMetrics generateUlyssesRecMetrics(int timeTaken) {
        UlyssesRecMetrics metrics = new UlyssesRecMetrics();
        metrics.setGetRecommendationDurationMS(timeTaken);
        return metrics;
    }

    @Around("execution(* com.latticeengines.ulysses.controller.TalkingPointResource.*(..))")
    public Object ulyssesTalkingPointsMonitor(ProceedingJoinPoint joinPoint) throws Throwable {

        Object retval;
        long timestamp = System.currentTimeMillis();
        retval = joinPoint.proceed();
        long timeTaken = System.currentTimeMillis() - timestamp;

        UlyssesTalkingPointsMetrics metrics = generateUlyssesTalkingPointsMetrics((int) timeTaken);
        UlyssesTalkingPointsMeasurement measurement = new UlyssesTalkingPointsMeasurement(metrics);
        metricService.write(MetricDB.ULYSSES, measurement);

        return retval;

    }

    private UlyssesTalkingPointsMetrics generateUlyssesTalkingPointsMetrics(int timeTaken) {
        UlyssesTalkingPointsMetrics metrics = new UlyssesTalkingPointsMetrics();
        metrics.setGetTalkingPointsDurationMS(timeTaken);
        return metrics;
    }

}
