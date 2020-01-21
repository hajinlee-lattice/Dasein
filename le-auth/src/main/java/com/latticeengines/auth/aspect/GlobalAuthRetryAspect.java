package com.latticeengines.auth.aspect;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Aspect
public class GlobalAuthRetryAspect {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthRetryAspect.class);

    @Around("execution(public * com.latticeengines.auth.exposed.entitymanager.impl.GlobalAuthAuthenticationEntityMgrImpl.find*(..)) "
            + "|| execution(public * com.latticeengines.auth.exposed.entitymanager.impl.GlobalAuthUserEntityMgrImpl.find*(..))")
    public Object globalAuthApiRetry(ProceedingJoinPoint joinPoint) throws Throwable {

        long startTime = System.currentTimeMillis();
        Object retVal = null;
        int retry = 2;
        try {
            while (retry > 0) {
                try {
                    retVal = joinPoint.proceed();
                    if (retVal != null) {
                        return retVal;
                    }
                } catch (Throwable ex) {
                    log.warn(String.format("There's exception happening! retries=%s", retry), ex);
                }
                if (retry > 1) {
                    Thread.sleep(200);
                }
                retry--;
            }
        } catch (Exception e) {
            log.warn(String.format("exception from retry"), e);
        } finally {
            long endTime = System.currentTimeMillis();
            String queryParameters = getQueryParameters();
            log.info(String.format("global auth method=%s threadName=%s ElapsedTime=%s queryParameters=%s",
                    joinPoint.getSignature().toShortString(), Thread.currentThread().getName(),
                    DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"), queryParameters));
        }
        return retVal;


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
