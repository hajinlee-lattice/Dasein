package com.latticeengines.playmaker.aspect;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.monitor.exposed.ratelimit.RateLimitException;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.playmaker.service.PlaymakerRateLimitingService;

@Aspect
public class PlaymakerRateLimitingAspect {

    @Inject
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Inject
    private PlaymakerRateLimitingService playmakerRateLimitingService;

    @Before("execution(* com.latticeengines.playmaker.controller.RecommendationResource.*(..)) ")
    public void rateLimitPlaymakerRequest(JoinPoint joinPoint){
        /*
        if (joinPoint.getArgs().length > 0 && joinPoint.getArgs()[0] instanceof HttpServletRequest) {
            HttpServletRequest request = (HttpServletRequest)joinPoint.getArgs()[0];
            String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);

            RateLimitedAcquisition acquisition = playmakerRateLimitingService.acquirePlaymakerRequest(tenantName,false);

            if (!acquisition.isAllowed()) {
                String errorMessage = "";
                if (acquisition.getExceedingQuotas() != null && !acquisition.getExceedingQuotas().isEmpty()) {
                    errorMessage += "Exceeding quotas: " + StringUtils.join(acquisition.getExceedingQuotas(), ",");
                }
                throw new RateLimitException(LedpCode.LEDP_00005, new String[]{errorMessage}, null);
            }
        }
        */
    }
}
