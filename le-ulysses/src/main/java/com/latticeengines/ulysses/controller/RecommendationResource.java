package com.latticeengines.ulysses.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;
import com.latticeengines.ulysses.utils.DanteFormatter;
import com.latticeengines.ulysses.utils.RecommendationDanteFormatter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Recommendation", description = "Common REST resource to lookup recommendations")
@RestController
@RequestMapping("/recommendations")
public class RecommendationResource {
    private static final Logger log = LoggerFactory.getLogger(TalkingPointResource.class);

    @Inject
    private LpiPMRecommendation lpiPMRecommendation;

    @Inject
    @Qualifier(RecommendationDanteFormatter.Qualifier)
    private DanteFormatter<Recommendation> recommendationDanteFormatter;

    @GetMapping(value = "/{recommendationId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a recommendation by recommendationId")
    public Recommendation getRecommendationById(@PathVariable String recommendationId) {
        return lpiPMRecommendation.getRecommendationById(recommendationId);
    }

    @GetMapping(value = "/{recommendationId}/danteformat", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a recommendation in legacy Dante format by Id")
    public FrontEndResponse<String> getRecommendationByIdInDanteFormat(@PathVariable String recommendationId) {
        try {
            return new FrontEndResponse<>(recommendationDanteFormatter.format(getRecommendationById(recommendationId)));
        } catch (LedpException le) {
            log.error("Failed to get recommendation data", le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get recommendation data", e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

}
