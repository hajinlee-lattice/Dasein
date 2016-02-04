package com.latticeengines.propdata.match.aspect;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.service.impl.MatchContext;

@Component
@Aspect
public class MatchStepAspect {

    public static Log log = LogFactory.getLog(MatchStepAspect.class);

    ThreadLocal<String> tracker = new ThreadLocal<>();

    @Around("@annotation(com.latticeengines.propdata.match.annotation.MatchStep)")
    public Object logMatchStep(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getKind().contains("execution")) {
            try {
                return logMatchStepMetrics(joinPoint);
            } finally {
                tracker.remove();
            }
        } else {
            return joinPoint.proceed();
        }
    }

    private Object logMatchStepMetrics(ProceedingJoinPoint joinPoint) throws Throwable {
        Long startTime = System.currentTimeMillis();

        Object retVal = joinPoint.proceed();

        Long elapsedTime = System.currentTimeMillis() - startTime;

        Object[] allObjs = combineArgsAndReturn(joinPoint.getArgs(), retVal);

        String logMsg = String.format("MatchStep=%s ElapsedTime=%d ms", joinPoint.getSignature().toShortString(),
                elapsedTime);

        String tenantId = getTenantId(allObjs);
        if (tenantId != null) {
            logMsg += " TenantId=" + tenantId;
        }

        Integer rows = getRequestedRows(allObjs);
        if (rows != null) {
            logMsg += " RowsRequested=" + rows;
        }

        String trackId = tracker.get();
        if (trackId == null) {
            trackId = getRootOperationUID(allObjs);
            tracker.set(trackId);
        }
        if (trackId != null) {
            logMsg += " RootOperationUID=" + trackId;
        }

        log.info(logMsg);

        return retVal;
    }

    private Object[] combineArgsAndReturn(Object[] args, Object retVal) {
        Object[] allObjs = new Object[args.length + 1];
        for (int i = 0; i < args.length; i++) {
            allObjs[i] = args[i];
        }
        allObjs[args.length] = retVal;
        return allObjs;
    }

    private String getRootOperationUID(Object[] args) {
        for (Object arg : args) {
            if (arg instanceof MatchContext) {
                MatchContext matchContext = (MatchContext) arg;
                return matchContext.getOutput().getRootOperationUID();
            }
        }
        return null;
    }

    private Integer getRequestedRows(Object[] args) {
        for (Object arg : args) {
            if (arg instanceof MatchContext) {
                MatchContext matchContext = (MatchContext) arg;
                return matchContext.getOutput().getStatistics().getRowsRequested();
            } else if (arg instanceof MatchInput) {
                MatchInput matchInput = (MatchInput) arg;
                return matchInput.getData().size();
            } else if (arg instanceof MatchOutput) {
                MatchOutput matchOutput = (MatchOutput) arg;
                return matchOutput.getStatistics().getRowsRequested();
            }
        }

        return null;
    }

    private String getTenantId(Object[] args) {
        for (Object arg : args) {
            if (arg instanceof MatchContext) {
                MatchContext matchContext = (MatchContext) arg;
                return matchContext.getOutput().getSubmittedBy().getId();
            } else if (arg instanceof MatchInput) {
                MatchInput matchInput = (MatchInput) arg;
                return matchInput.getTenant().getId();
            }
        }
        return null;
    }

}
