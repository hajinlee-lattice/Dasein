package com.latticeengines.ulysses.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Recommendation", description = "Common REST resource to lookup recommendations")
@RestController
@RequestMapping("/recommendations")
public class RecommendationResource {

    @Autowired
    private LpiPMRecommendation lpiPMRecommendation;

    @RequestMapping(value = "/{recommendationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a recommendation by recommendationId")
    public Recommendation getTalkingPointById(@PathVariable String recommendationId) {
        return lpiPMRecommendation.getRecommendationById(recommendationId);
    }

}
