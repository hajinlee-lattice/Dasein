package com.latticeengines.playmaker.aspect;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.playmaker.entitymgr.impl.JdbcTempalteFactoryImpl;

@Aspect
public class RecommendationRetryAspect {

    public static Log log = LogFactory.getLog(RecommendationRetryAspect.class);

    @Autowired
    private JdbcTempalteFactoryImpl templateFactory;

    @Around("execution(public * com.latticeengines.playmaker.entitymgr.impl.PlaymakerRecommendationEntityMgrImpl.*(..)) && args(tenantName, ..)")
    public Object playMakerRecommendationApiRetry(ProceedingJoinPoint joinPoint, String tenantName) throws Throwable {

        long startTime = System.currentTimeMillis();
        try {
            Object retVal = null;
            int retries = 2;
            Exception exception = null;
            while (retries > 0) {
                try {
                    retVal = joinPoint.proceed();
                    return retVal;
                } catch (Exception ex) {
                    log.warn("There's exception happening!, retries=" + retries, ex);
                    exception = ex;
                    templateFactory.removeTemplate(tenantName);
                    retries--;
                }
            }

            throw new LedpException(LedpCode.LEDP_22007, exception);

        } finally {
            long endTime = System.currentTimeMillis();
            log.info(String.format("Recommendation method=%s tenantName=%s threadName=%s, ElapsedTime=%s", joinPoint
                    .getSignature().toShortString(), tenantName, Thread.currentThread().getName(), DurationFormatUtils
                    .formatDuration(endTime - startTime, "HH:mm:ss:SS")));
        }

    }
}
