package com.latticeengines.scoringapi.controller;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.rest.RequestIdUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreUtils;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;
import com.latticeengines.scoringapi.score.ScoreRequestConfigProcessor;
import com.latticeengines.scoringinternalapi.controller.BaseScoring;
import com.latticeengines.security.exposed.Constants;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "external", description = "REST resource for external Score Requests")
@RestController
@RequestMapping(value = "/external")
public class ScoreExternalResource extends BaseScoring {

    @Inject
    private BatonService batonService;

    @Inject
    private ScoreRequestConfigProcessor srcProcessor;

    @RequestMapping(value = "/record/{configId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a record from External System")
    public ScoreResponse scorePercentileRecord(HttpServletRequest request, @PathVariable(name="configId") String configId,
            @RequestBody ScoreRequest scoreRequest) {
        if (StringUtils.isBlank(configId)) {
            throw new ScoringApiException(LedpCode.LEDP_18202);
        }
        String secretKey = request.getHeader(Constants.LATTICE_SECRET_KEY_HEADERNAME);
        if (StringUtils.isBlank(secretKey)) {
            throw new ScoringApiException(LedpCode.LEDP_18199);
        }

        if (scoreRequest == null || scoreRequest.getRecord() == null) {
            throw new ScoringApiException(LedpCode.LEDP_18201);
        }
        ScoringRequestConfigContext srcContext = srcProcessor.getScoreRequestConfigContext(configId, secretKey);
        CustomerSpace customerSpace = CustomerSpace.parse(srcContext.getTenantId());
        scoreRequest = srcProcessor.preProcessScoreRequestConfig(scoreRequest, srcContext);
        String requestId = RequestIdUtils.getRequestIdentifierId(request);
        return scorePercentileRecord(request, scoreRequest, customerSpace, //
                ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace), //
                false, requestId, false, false);
    }
}
