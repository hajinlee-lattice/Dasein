package com.latticeengines.ulysses.controller;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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

    @Inject
    private LpiPMRecommendation lpiPMRecommendation;

    @Inject
    @Qualifier(RecommendationDanteFormatter.Qualifier)
    private DanteFormatter<Recommendation> recommendationDanteFormatter;

    @RequestMapping(value = "/{recommendationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a recommendation by recommendationId")
    public Recommendation getRecommendationById(@PathVariable String recommendationId) {
        return lpiPMRecommendation.getRecommendationById(recommendationId);
    }

    @RequestMapping(value = "/{recommendationId}/danteformat", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a recommendation in legacy Dante format by Id")
    public FrontEndResponse<String> getRecommendationByIdInDanteFormat(@PathVariable String recommendationId) {
        try {
            return new FrontEndResponse<>(recommendationDanteFormatter.format(getRecommendationById(recommendationId)));
        } catch (LedpException le) {
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
    }

}
