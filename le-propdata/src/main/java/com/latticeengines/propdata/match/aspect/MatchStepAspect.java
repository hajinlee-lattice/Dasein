package com.latticeengines.propdata.match.aspect;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.impl.MatchContext;

@Component
@Aspect
public class MatchStepAspect {

    public static Log log = LogFactory.getLog(MatchStepAspect.class);

    private ThreadLocal<String> tracker = new ThreadLocal<>();
    private ConcurrentMap<String, Long> logThreshold = new ConcurrentHashMap<>();

    @Around("@annotation(com.latticeengines.propdata.match.annotation.MatchStep)")
    public Object logMatchStep(ProceedingJoinPoint joinPoint) throws Throwable {
        if (joinPoint.getKind().contains("execution")) {
            return logMatchStepMetrics(joinPoint);
        } else {
            return joinPoint.proceed();
        }
    }

    private Object logMatchStepMetrics(ProceedingJoinPoint joinPoint) throws Throwable {
        Long startTime = System.currentTimeMillis();
        String signature = joinPoint.getSignature().toShortString();

        Object retVal = joinPoint.proceed();
        Long elapsedTime = System.currentTimeMillis() - startTime;
        String logMsg = String.format("MatchStep=%s TimeElapsedMs=%d", signature, elapsedTime);

        String trackId = tracker.get();
        Object[] allObjs = combineArgsAndReturn(joinPoint.getArgs(), retVal);
        String uid = getRootOperationUID(allObjs);
        if (uid != null && !uid.equalsIgnoreCase(trackId)) {
            trackId = uid;
        }

        if (trackId != null) {
            logMsg += " RootOperationUID=" + trackId;
            tracker.set(trackId);
        }

        upsertAndRetrieveThreshold((MethodSignature) joinPoint.getSignature());
        Long threshold = logThreshold.get(signature);
        if (threshold <= elapsedTime) {
            log.info(logMsg);
        }
        return retVal;
    }

    private Long upsertAndRetrieveThreshold(MethodSignature signature) {
        if (!logThreshold.containsKey(signature.toShortString())) {
            Method method = signature.getMethod();
            MatchStep step = method.getAnnotation(MatchStep.class);
            logThreshold.putIfAbsent(signature.toShortString(), step.threshold());
        }
        return logThreshold.get(signature.toShortString());
    }

    private Object[] combineArgsAndReturn(Object[] args, Object retVal) {
        Object[] allObjs = new Object[args.length + 1];
        System.arraycopy(args, 0, allObjs, 0, args.length);
        allObjs[args.length] = retVal;
        return allObjs;
    }

    private String getRootOperationUID(Object[] args) {
        for (Object arg : args) {
            if (arg instanceof MatchContext) {
                MatchContext matchContext = (MatchContext) arg;
                return matchContext.getOutput().getRootOperationUID();
            } else if (arg instanceof MatchOutput) {
                MatchOutput output = (MatchOutput) arg;
                if (StringUtils.isNotEmpty(output.getRootOperationUID())) {
                    return output.getRootOperationUID();
                }
            } else if (arg instanceof MatchInput) {
                MatchInput input = (MatchInput) arg;
                if (input.getUuid() != null) {
                    return input.getUuid().toString().toUpperCase();
                }
            }
        }
        return null;
    }

}
