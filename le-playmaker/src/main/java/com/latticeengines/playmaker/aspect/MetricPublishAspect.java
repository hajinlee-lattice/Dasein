package com.latticeengines.playmaker.aspect;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.playmaker.measurements.PlaymakerAccExtMeasurement;
import com.latticeengines.playmaker.measurements.PlaymakerAccExtMetrics;
import com.latticeengines.playmaker.measurements.PlaymakerPlayMeasurement;
import com.latticeengines.playmaker.measurements.PlaymakerPlayMetrics;
import com.latticeengines.playmaker.measurements.PlaymakerRecMeasurement;
import com.latticeengines.playmaker.measurements.PlaymakerRecMetrics;

@Aspect
public class MetricPublishAspect {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(MetricPublishAspect.class);

    @Inject
    protected MetricService metricService;

    @Around("execution(* com.latticeengines.playmaker.entitymgr.impl.PlaymakerRecommendationEntityMgrImpl.getRecommendations(..)) && args(tenantName, lookupSource, start, offset, maximum, syncDestination, playIds, orgInfo, ..)")
    public Object playmakerRecommendationApiMonitor(ProceedingJoinPoint joinPoint, String tenantName,
            String lookupSource, long start, int offset, int maximum, int syncDestination, List<String> playIds,
            Map<String, String> orgInfo) throws Throwable {

        Object retval = null;
        long timestamp = System.currentTimeMillis();
        retval = joinPoint.proceed();
        long timeTaken = System.currentTimeMillis() - timestamp;
        SynchronizationDestinationEnum syncDestEnum = SynchronizationDestinationEnum.fromIntValue(syncDestination);

        Pair<String, String> effectiveOrgInfo = null;
        String destinationOrgId = null;
        String destinationSysType = null;
        if (MapUtils.isNotEmpty(orgInfo)) {
            effectiveOrgInfo = getDestInfoFromOrgInfo(orgInfo);

            destinationOrgId = effectiveOrgInfo.getLeft();
            destinationSysType = effectiveOrgInfo.getRight();
        }

        PlaymakerRecMetrics metrics = generateRecommendationMetrics(tenantName, (int) timeTaken, maximum,
                destinationOrgId, destinationSysType, syncDestEnum, lookupSource);
        PlaymakerRecMeasurement measurement = new PlaymakerRecMeasurement(metrics);
        metricService.write(MetricDB.PLAYMAKER, measurement);

        return retval;

    }

    private PlaymakerRecMetrics generateRecommendationMetrics(String tenantName, int timeTaken, int maximum,
            String destinationOrgId, String destinationSysType, SynchronizationDestinationEnum syncDestEnum,
            String lookupSource) {
        PlaymakerRecMetrics metrics = new PlaymakerRecMetrics();
        metrics.setTenantId(tenantName);
        metrics.setGetRecommendationDurationMS(timeTaken);
        metrics.setMaximum(maximum);
        metrics.setDestinationOrgId(destinationOrgId);
        metrics.setDestinationSysType(destinationSysType);
        metrics.setSyncDestination(syncDestEnum.name());
        metrics.setDataPlatform(lookupSource);

        return metrics;
    }

    @Around("execution(* com.latticeengines.playmaker.entitymgr.impl.PlaymakerRecommendationEntityMgrImpl.getPlays(..)) && args(tenantName, lookupSource, start, offset, maximum, playgroupIds, ..)")
    public Object playmakerPlayApiMonitor(ProceedingJoinPoint joinPoint, String tenantName, String lookupSource,
            long start, int offset, int maximum, List<Integer> playgroupIds) throws Throwable {

        Object retval = null;
        long timestamp = System.currentTimeMillis();
        retval = joinPoint.proceed();
        long timeTaken = System.currentTimeMillis() - timestamp;

        PlaymakerPlayMetrics metrics = generatePlayMetrics(tenantName, (int) timeTaken, lookupSource);
        PlaymakerPlayMeasurement measurement = new PlaymakerPlayMeasurement(metrics);
        metricService.write(MetricDB.PLAYMAKER, measurement);

        return retval;

    }

    private PlaymakerPlayMetrics generatePlayMetrics(String tenantName, int timeTaken, String lookupSource) {
        PlaymakerPlayMetrics metrics = new PlaymakerPlayMetrics();
        metrics.setTenantId(tenantName);
        metrics.setGetPlayDurationMS(timeTaken);
        metrics.setDataPlatform(lookupSource);

        return metrics;
    }

    @Around("execution(* com.latticeengines.playmaker.entitymgr.impl.PlaymakerRecommendationEntityMgrImpl.getAccountExtensions(..)) && args(tenantName, lookupSource, start, offset, maximum, accountIds, filterBy, recStart, columns, hasSfdcContactId, orgInfo, ..)")
    public Object playmakerAccountExtensionMonitor(ProceedingJoinPoint joinPoint, String tenantName,
            String lookupSource, Long start, int offset, int maximum, List<String> accountIds, String filterBy,
            Long recStart, String columns, boolean hasSfdcContactId, Map<String, String> orgInfo) throws Throwable {

        Object retval = null;
        long timestamp = System.currentTimeMillis();
        retval = joinPoint.proceed();
        long timeTaken = System.currentTimeMillis() - timestamp;

        Pair<String, String> effectiveOrgInfo = null;
        String destinationOrgId = null;
        String destinationSysType = null;
        if (MapUtils.isNotEmpty(orgInfo)) {
            effectiveOrgInfo = getDestInfoFromOrgInfo(orgInfo);

            destinationOrgId = effectiveOrgInfo.getLeft();
            destinationSysType = effectiveOrgInfo.getRight();
        }

        PlaymakerAccExtMetrics metrics = generateAccountExtensionMetrics(tenantName, (int) timeTaken, maximum,
                destinationOrgId, destinationSysType, lookupSource);
        PlaymakerAccExtMeasurement measurement = new PlaymakerAccExtMeasurement(metrics);
        metricService.write(MetricDB.PLAYMAKER, measurement);

        return retval;

    }

    private PlaymakerAccExtMetrics generateAccountExtensionMetrics(String tenantName, int timeTaken, int maximum,
            String destinationOrgId, String destinationSysType, String lookupSource) {
        PlaymakerAccExtMetrics metrics = new PlaymakerAccExtMetrics();
        metrics.setTenantId(tenantName);
        metrics.setGetAccountExtensionDurationMS(timeTaken);
        metrics.setMaximum(maximum);
        metrics.setDestinationOrgId(destinationOrgId);
        metrics.setDestinationSysType(destinationSysType);
        metrics.setDataPlatform(lookupSource);

        return metrics;
    }

    private Pair<String, String> getDestInfoFromOrgInfo(Map<String, String> orgInfo) {
        Pair<String, String> effectiveOrgInfo = null;
        if (StringUtils.isNotBlank(orgInfo.get(CDLConstants.ORG_ID))
                && StringUtils.isNotBlank(orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE))) {
            effectiveOrgInfo = new ImmutablePair<String, String>(orgInfo.get(CDLConstants.ORG_ID).trim(),
                    orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE).trim());
        }

        return effectiveOrgInfo;
    }

}
