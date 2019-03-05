package com.latticeengines.playmaker.aspect;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.playmaker.entitymgr.impl.JdbcTemplateFactoryImpl;

@Aspect
public class RecommendationRetryAspect {

    public static Logger log = LoggerFactory.getLogger(RecommendationRetryAspect.class);

    @Inject
    private JdbcTemplateFactoryImpl templateFactory;

    @Around("execution(public * com.latticeengines.playmaker.entitymgr.impl.PlaymakerRecommendationEntityMgrImpl.*(..)) && args(tenantName, ..)")
    public Object playMakerRecommendationApiRetry(ProceedingJoinPoint joinPoint, String tenantName) throws Throwable {

        long startTime = System.currentTimeMillis();
        try {
            Object retVal = null;
            int retries = 2;
            Throwable t = null;
            while (retries > 0) {
                try {
                    retVal = joinPoint.proceed();
                    return retVal;
                } catch (Throwable ex) {
                    log.warn(String.format("There's exception happening!, retries=%s tenantName=%s", retries, tenantName), ex);
                    t = ex;
                    templateFactory.removeTemplate(tenantName);
                    retries--;
                }
            }

            throw new LedpException(LedpCode.LEDP_22007, t);

        } finally {
            long endTime = System.currentTimeMillis();
            String queryParameters = getQueryParameters();
            log.info(String.format("Recommendation method=%s tenantName=%s threadName=%s ElapsedTime=%s queryParameters=%s",
                    joinPoint.getSignature().toShortString(), tenantName, Thread.currentThread().getName(),
                    DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"), queryParameters));
        }

    }

    private String getQueryParameters() {
        ServletRequestAttributes requestAttributes = (ServletRequestAttributes) RequestContextHolder
                .getRequestAttributes();
        if (requestAttributes != null) {
            HttpServletRequest request = requestAttributes.getRequest();
            return request.getQueryString();
        }
        return "";
    }
}
